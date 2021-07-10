from celery import Task


class RetryTask(Task):
    autoretry_for = (Exception,)
    max_retries = None
    retry_backoff = True
    retry_backoff_max = 600
    retry_jitter = False
