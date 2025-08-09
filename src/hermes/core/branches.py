"""
Defines data structures for creating structured Markdown documents.

This module provides two main components:
1.  Branches: An immutable data class that holds the content for a document,
    structured as a title and a dictionary of sections with listed items.
2.  BranchesBuilder: A builder class to construct a Branches object in a step-by-step
    and readable manner.
"""

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Self # Use Self for fluent interface type hints

@dataclass(frozen=True)
class Branches:
    """
    An immutable data class representing a structured document.

    Attributes:
        identifier (str): A unique string used as the filename for the output.
        title (str): The main title of the document (rendered as H1).
        table (dict[str, list[str]]): A dictionary where keys are section
                                     titles (H2) and values are lists of
                                     items for that section.
    """
    identifier: str
    title: str
    table: dict[str, list[str]]

    def render(self) -> str:
        """
        Renders the entire document structure into a single Markdown string.

        This method is the core rendering logic, used by other methods like
        render_markdown.

        Returns:
            str: The fully formatted Markdown content as a string.
        """
        text_parts = []
        text_parts.append(f"# {self.title}\n\n")

        for key, values in self.table.items():
            text_parts.append(f"## {key}\n\n")
            # Enumerate starting from 1 for a numbered list
            for cursor, element in enumerate(values, 1):
                text_parts.append(f"{cursor}. {element}\n")
            # Add a newline for spacing between sections
            text_parts.append("\n")

        return "".join(text_parts)

    def render_markdown(self, container: Path) -> None:
        """
        Renders the document to a Markdown file in a specified directory.

        The output filename will be '{self.identifier}.md'.

        Args:
            container (Path): The parent directory where the file will be saved.
        """
        # Ensure the target directory exists before writing the file.
        container.mkdir(parents=True, exist_ok=True)

        # Construct the full path for the output file.
        resource = container / f"{self.identifier}.md"

        with open(resource, "w", encoding="utf-8") as text_file:
            # Reuse the main render method to get the content.
            text_file.write(self.render())


class BranchesBuilder:
    """
    A builder for constructing a Branches object using a fluent interface.

    This pattern allows for creating a complex object step by step.
    Example:
        builder = BranchesBuilder()
        branches = (builder.set_identifier("my-doc")
                           .set_title("My Document")
                           .add("Section 1", "Item A")
                           .add("Section 1", "Item B")
                           .build())
    """
    def __init__(self) -> None:
        """Initializes the builder with empty attributes."""
        self._identifier: str | None = None
        self._title: str | None = None
        # defaultdict makes it easy to append to lists for new keys.
        self._table: defaultdict[str, list[str]] = defaultdict(list)

    def set_identifier(self, identifier: str) -> Self:
        """
        Sets the identifier for the Branches object.

        Args:
            identifier (str): The unique identifier.

        Returns:
            Self: The builder instance for method chaining.
        """
        self._identifier = identifier
        return self

    def set_title(self, title: str) -> Self:
        """
        Sets the title for the Branches object.

        Args:
            title (str): The main document title.

        Returns:
            Self: The builder instance for method chaining.
        """
        self._title = title
        return self

    def add(self, key: str, value: str) -> Self:
        """
        Adds an item to a section in the document.

        If the section (key) does not exist, it will be created.

        Args:
            key (str): The title of the section.
            value (str): The item to add to the section's list.

        Returns:
            Self: The builder instance for method chaining.
        """
        self._table[key].append(value)
        return self

    def build(self) -> Branches:
        """
        Constructs and returns the final Branches object.

        This method validates that all required fields have been set before
        creating the object.

        Returns:
            Branches: The immutable, fully constructed Branches object.

        Raises:
            ValueError: If the identifier or title has not been set.
        """
        if self._identifier is None:
            raise ValueError("Identifier must be set before building.")
        if self._title is None:
            raise ValueError("Title must be set before building.")

        # Create and return the immutable Branches instance from the builder's state.
        return Branches(
            identifier=self._identifier,
            title=self._title,
            table=dict(self._table) # Convert defaultdict back to a regular dict
        )
