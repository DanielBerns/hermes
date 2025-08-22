import argparse
from typing import Any, Dict, List, Optional, Tuple


class CLI:
    """
    A wrapper class for argparse.ArgumentParser to simplify the creation of
    Command Line Interfaces.

    This class provides methods to add arguments and retrieve the parsed
    arguments as a dictionary.
    """

    def __init__(self, description: Optional[str] = None) -> None:
        """
        Initializes the CLI class.

        Args:
            description (Optional[str]): A description of what the program does.
                                         This will be shown in the help message.
                                         Defaults to None.
        """
        self._parser = argparse.ArgumentParser(description=description)

    @property
    def parser(self) -> argparse.ArgumentParser:
        """
        Provides access to the underlying ArgumentParser instance.

        Returns:
            argparse.ArgumentParser: The ArgumentParser instance.
        """
        return self._parser

    @property
    def arguments(self) -> dict[str, Any]:
        """
        Parses the command-line arguments and returns them as a dictionary.

        Returns:
            Dict[str, Any]: A dictionary where keys are argument names
                            and values are the parsed argument values.
        """
        # Get the parsed arguments
        return vars(self.parser.parse_args())

    def add_argument(self, *args: Any, **kwargs: Any) -> None:
        """
        Adds an argument to the parser.

        This method is a wrapper around ArgumentParser.add_argument().
        It allows for adding positional or optional arguments.

        Args:
            *args (Any): Variable length argument list (e.g., 'filename' or '-f', '--file').
            **kwargs (Any): Arbitrary keyword arguments (e.g., help='Help text', type=str).
                           Refer to the argparse.ArgumentParser.add_argument()
                           documentation for all possible keyword arguments.

        Example:
            cli.add_argument('input_file', help='The input file to process')
            cli.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
            cli.add_argument('--count', type=int, default=1, help='Number of iterations')
        """
        self.parser.add_argument(*args, **kwargs)

    def add_arguments(
        self, arguments_to_add: List[Tuple[Tuple[Any, ...], Dict[str, Any]]]
    ) -> None:
        """
        Adds multiple arguments to the parser from a list of definitions.

        Each definition in the list should be a tuple containing two elements:
        1. A tuple of positional arguments for `add_argument` (e.g., ('-f', '--file')).
        2. A dictionary of keyword arguments for `add_argument` (e.g., {'help': 'The file to process', 'required': True}).

        Args:
            arguments_to_add (List[Tuple[Tuple[Any, ...], Dict[str, Any]]]):
                A list of argument definitions.

        Example:
            definitions = [
                (('input_file',), {'help': 'The input file'}),
                (('-o', '--output'), {'help': 'The output file', 'default': 'output.txt'})
            ]
            cli.add_arguments(definitions)
        """
        for args_tuple, kwargs_dict in arguments_to_add:
            self.parser.add_argument(*args_tuple, **kwargs_dict)
