import logging

def setup_log(name: str = "INFO") -> None:
    logging.basicConfig(
        level = name.upper(),
        format="%(asctime)s %(levelname)s %(name)s:%(lineno)d %(message)s"
    )