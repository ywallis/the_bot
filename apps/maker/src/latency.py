"""Latency model for the simulated broker, built from measured records.

Every placement the order manager makes leaves a ``LatencyRecord`` on
``oms:latency`` with four local timestamps: when the strategy created the
intent, when the order manager read it, when the request left for the broker
and when the broker's reply came back. A backtest needs three delays from
them, per venue:

- ``oms_lag``: creation to order manager read, the strategy-to-bus leg;
- ``send_lag``: order manager read to broker send, the time inside the order
  manager while nothing else was in flight for that strategy;
- ``rtt``: broker send to broker reply, the whole REST round trip. Half of
  it is taken as the one-way trip to the venue, since the records carry no
  venue-side timestamp that could split it better.

The model draws whole samples from the measured set rather than fitting a
distribution, so its tails are the tails that were seen. A venue without a
single measured placement has no model, and the backtest is refused unless a
round trip is assumed for it explicitly; that assumption is carried into the
report, because a cross-venue conclusion resting on it is worth less than one
resting on data. See ``docs/design/event-driven-framework.md`` section 9.
"""

import logging
import random
import statistics
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from apps.shared.src.events import LATENCY_STREAM, LatencyRecord, decode
from apps.shared.src.streams import recording_files, stream_path

logger = logging.getLogger(__name__)

NS_PER_MS = 1_000_000
# What an assumed model puts on the legs the assumption says nothing about:
# the median strategy-to-order-manager leg seen live, and no queueing.
ASSUMED_OMS_LAG_NS = 600_000
ASSUMED_SEND_LAG_NS = 0

MEASURED = "measured"
ASSUMED = "assumed"


@dataclass(frozen=True, slots=True)
class LatencySample:
    """
    The delays of one placement, nanoseconds.

    Attributes
    ----------
    oms_lag_ns : int
        Intent creation to order manager read.
    send_lag_ns : int
        Order manager read to broker send.
    rtt_ns : int
        Broker send to broker reply.
    """

    oms_lag_ns: int
    send_lag_ns: int
    rtt_ns: int

    @property
    def one_way_ns(self) -> int:
        """
        Return the assumed trip to the venue, half the round trip.

        Returns
        -------
        int
            Nanoseconds.
        """
        return self.rtt_ns // 2


def sample_of(record: LatencyRecord) -> LatencySample | None:
    """
    Turn a latency record into a sample, if it is complete.

    Parameters
    ----------
    record : LatencyRecord
        The record.

    Returns
    -------
    LatencySample | None
        The sample, or None if the record lacks a timestamp or runs
        backwards, which a clock step during the live run can produce.
    """
    if (
        record.ts_oms_recv is None
        or record.ts_broker_send is None
        or record.ts_broker_ack is None
    ):
        return None
    sample = LatencySample(
        oms_lag_ns=record.ts_oms_recv - record.ts_created,
        send_lag_ns=record.ts_broker_send - record.ts_oms_recv,
        rtt_ns=record.ts_broker_ack - record.ts_broker_send,
    )
    if min(sample.oms_lag_ns, sample.send_lag_ns, sample.rtt_ns) < 0:
        return None
    return sample


class VenueLatency:
    """
    The latency of one venue: a set of samples and how they were obtained.

    Attributes
    ----------
    venue : str
        CCXT short id.
    samples : list[LatencySample]
        The samples drawn from.
    source : str
        ``MEASURED`` or ``ASSUMED``.
    """

    def __init__(
        self, venue: str, samples: Iterable[LatencySample], source: str, seed: int
    ) -> None:
        """
        Initialize the venue's model.

        Parameters
        ----------
        venue : str
            CCXT short id.
        samples : Iterable[LatencySample]
            At least one sample.
        source : str
            ``MEASURED`` or ``ASSUMED``.
        seed : int
            Seed of the generator that picks samples, so a run repeats.

        Raises
        ------
        ValueError
            If there are no samples.
        """
        self.venue = venue
        self.samples = list(samples)
        if not self.samples:
            raise ValueError(f"No latency samples for {venue}")
        self.source = source
        self._random = random.Random(seed)

    def draw(self) -> LatencySample:
        """
        Draw one placement's delays.

        Returns
        -------
        LatencySample
            A sample picked uniformly from the measured set, so the three
            legs of one placement stay together.
        """
        return self._random.choice(self.samples)

    def summary(self) -> dict[str, float | int | str]:
        """
        Summarise the model for the report.

        Returns
        -------
        dict[str, float | int | str]
            Source, sample count and round trip median, 95th percentile and
            maximum in milliseconds.
        """
        rtts = sorted(sample.rtt_ns for sample in self.samples)
        p95 = rtts[min(len(rtts) - 1, int(0.95 * len(rtts)))]
        return {
            "source": self.source,
            "samples": len(rtts),
            "rtt_median_ms": statistics.median(rtts) / NS_PER_MS,
            "rtt_p95_ms": p95 / NS_PER_MS,
            "rtt_max_ms": rtts[-1] / NS_PER_MS,
        }


class LatencyModel:
    """
    Per-venue latency, measured where possible and assumed where declared.

    Attributes
    ----------
    venues : dict[str, VenueLatency]
        The model per venue id.
    """

    def __init__(self, seed: int = 0) -> None:
        """
        Initialize an empty model.

        Parameters
        ----------
        seed : int
            Base seed; each venue derives its own so adding one does not
            change another's draws.
        """
        self.seed = seed
        self.venues: dict[str, VenueLatency] = {}

    def add_records(self, records: Iterable[LatencyRecord]) -> int:
        """
        Add measured records, grouped by venue.

        Parameters
        ----------
        records : Iterable[LatencyRecord]
            Records from ``oms:latency``.

        Returns
        -------
        int
            Number of usable samples added. A venue already assumed keeps
            its assumption; measured data is only added for venues without
            a model or with a measured one.
        """
        grouped: dict[str, list[LatencySample]] = {}
        for record in records:
            sample = sample_of(record)
            if sample is not None:
                grouped.setdefault(record.venue, []).append(sample)
        added = 0
        for venue, samples in grouped.items():
            existing = self.venues.get(venue)
            if existing is not None and existing.source == ASSUMED:
                continue
            combined = (existing.samples if existing is not None else []) + samples
            self.venues[venue] = VenueLatency(
                venue, combined, MEASURED, self._seed_for(venue)
            )
            added += len(samples)
        return added

    def assume(self, venue: str, rtt_ms: float) -> None:
        """
        Give a venue a constant round trip in place of measurements.

        Parameters
        ----------
        venue : str
            CCXT short id.
        rtt_ms : float
            The assumed round trip in milliseconds.
        """
        logger.warning(
            f"Assuming a {rtt_ms} ms round trip for {venue}; nothing measured stands "
            "behind any conclusion about this venue"
        )
        sample = LatencySample(
            ASSUMED_OMS_LAG_NS, ASSUMED_SEND_LAG_NS, int(rtt_ms * NS_PER_MS)
        )
        self.venues[venue] = VenueLatency(
            venue, [sample], ASSUMED, self._seed_for(venue)
        )

    def for_venue(self, venue: str) -> VenueLatency:
        """
        Return a venue's model.

        Parameters
        ----------
        venue : str
            CCXT short id.

        Returns
        -------
        VenueLatency
            The model.

        Raises
        ------
        KeyError
            If the venue has neither measurements nor an assumption.
        """
        try:
            return self.venues[venue]
        except KeyError:
            raise KeyError(
                f"No latency model for {venue}: nothing measured on oms:latency and no "
                "round trip assumed for it"
            ) from None

    def require(self, venues: Iterable[str]) -> None:
        """
        Check that every venue has a model.

        Parameters
        ----------
        venues : Iterable[str]
            Venue ids a backtest will place orders on.

        Raises
        ------
        KeyError
            For the first venue without one.
        """
        for venue in venues:
            self.for_venue(venue)

    def summary(self) -> dict[str, dict[str, float | int | str]]:
        """
        Summarise every venue for the report.

        Returns
        -------
        dict[str, dict[str, float | int | str]]
            ``VenueLatency.summary`` per venue.
        """
        return {venue: model.summary() for venue, model in sorted(self.venues.items())}

    def _seed_for(self, venue: str) -> int:
        # Not ``hash``: Python salts string hashes per process, and a backtest
        # must draw the same latencies on every run.
        return zlib.crc32(venue.encode()) ^ self.seed


def latency_records(root: Path) -> list[LatencyRecord]:
    """
    Read every latency record in a recording.

    Parameters
    ----------
    root : Path
        Recording root directory.

    Returns
    -------
    list[LatencyRecord]
        Records in recorded order, the whole recording rather than a
        backtest's range: more placements make a better model, and the
        venue's latency does not depend on which hour is being replayed.
    """
    # Imported here: the replayer imports the recorder and ties the whole
    # tool chain together, and this module is also used without it.
    from apps.maker.src.replayer import iter_stream

    directory = stream_path(root, LATENCY_STREAM)
    if not recording_files(directory):
        return []
    records: list[LatencyRecord] = []
    for record in iter_stream(LATENCY_STREAM, directory, None, None):
        event = decode(record.data)
        if isinstance(event, LatencyRecord):
            records.append(event)
    return records
