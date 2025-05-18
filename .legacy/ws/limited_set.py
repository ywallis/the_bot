from collections import deque

class LimitedSet:
    def __init__(self, max_size):
        self.max_size = max_size
        self.items = set()
        self.order = deque()

    def add(self, item):
        if item not in self.items:
            if len(self.order) == self.max_size:
                oldest_item = self.order.popleft()
                self.items.remove(oldest_item)
            self.order.append(item)
            self.items.add(item)

    def __contains__(self, item):
        return item in self.items

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.order)  # Optional: maintain insertion order

# # Example usage:
# limited_set = LimitedSet(50)
#
# # Adding items
# for i in range(60):
#     limited_set.add(i)
#
# print(len(limited_set))  # Output: 50
# print(list(limited_set))  # Shows the last 50 items added
#
# limited_set.add(249)
#
# print(list(limited_set))  # Shows the last 50 items added

