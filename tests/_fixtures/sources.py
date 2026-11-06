"""Source scripts shared by tests."""


def frame_script(columns: str) -> str:
    """A source script returning a fixed frame, given a dict literal of columns."""
    return (
        "class Frame(DataSource):\n"
        "    async def history(self, stream, symbol, start_ts, end_ts, limit):\n"
        f"        return pl.DataFrame({columns})\n"
    )
