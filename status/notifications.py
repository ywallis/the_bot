import smtplib
from config.config import EMAIL, PASSWORD


def send_email(subject, message):

    with smtplib.SMTP('smtp.gmail.com', 587) as email_out:
        email_out.starttls()
        email_out.login(EMAIL, PASSWORD)
        email_out.sendmail(EMAIL, ["yann.wallis@gmail.com"], f"Subject:{subject} \n\n {message}")
