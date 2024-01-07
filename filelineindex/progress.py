import sys
from typing import Callable


class Progress:
    """A generic progress reporting class."""

    def __init__(self, handler: Callable[[int], None]):
        """
        Initialize a new instance of the Progress class.

        :param handler: A callable that takes an integer representing the percentage progress.
        """
        self.__handler = handler
        self.__percentage = None

    def report(self, fraction: float) -> None:
        """
        Report the progress to the handler.

        :param fraction: The progress fraction in [0; 1].
        """
        percentage = int(100 * fraction)
        if percentage != self.__percentage:
            self.__percentage = percentage
            self.__handler(percentage)

    def report_start(self) -> None:
        """Report the progress of 0% to the handler."""
        if self.__percentage != 0:
            self.__percentage = 0
            self.__handler(0)

    def report_done(self) -> None:
        """Report the progress of 100% to the handler."""
        if self.__percentage != 100:
            self.__percentage = 100
            self.__handler(100)


class VoidProgress(Progress):
    """A progress class that does nothing when progress is reported."""

    def __init__(self):
        """
        Initialize a new instance of the VoidProgress class.
        """
        super().__init__(lambda _: None)


class ConsoleProgress(Progress):
    """A progress class that prints progress to the console."""

    def __init__(self):
        """
        Initialize a new instance of the ConsoleProgress class.
        """
        super().__init__(ConsoleProgress.__print_progress)

    @staticmethod
    def __print_progress(percentage) -> None:
        colors = (32, 92) if percentage == 100 else (34, 94)
        sys.stdout.write(
            f"\r\x1B[{colors[0]}mProgress:  \x1B[1m\x1B[{colors[1]}m{percentage}%\x1B[0m"
        )
        if percentage == 100:
            sys.stdout.write("\n")
