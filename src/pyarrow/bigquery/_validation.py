def positive_integer(value, name: str) -> None:
    """Reject invalid concurrency/chunk sizes before starting any remote work."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
