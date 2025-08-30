import pytest
from pathlib import Path

from hermes.core.search_table import SearchTable
from hermes.core.rows_io import read_rows, write_rows

# --- Tests for SearchTable ---

@pytest.fixture
def search_table() -> SearchTable:
    """Provides a SearchTable instance initialized with keys."""
    return SearchTable(keys=["id", "category"])

def test_search_table_insert_unique(search_table: SearchTable):
    """Tests that unique rows can be inserted successfully."""
    row1 = {"id": 1, "category": "A"}
    row2 = {"id": 2, "category": "B"}

    assert search_table.insert(row1) is True
    assert search_table.insert(row2) is True

def test_search_table_prevent_duplicates(search_table: SearchTable):
    """Tests that duplicate rows are not inserted."""
    row1 = {"id": 1, "category": "A"}
    row1_duplicate = {"id": 1, "category": "A"}

    assert search_table.insert(row1) is True
    assert search_table.insert(row1_duplicate) is False

def test_search_table_search_found(search_table: SearchTable):
    """Tests that an inserted row can be found."""
    row = {"id": 1, "category": "A"}
    search_table.insert(row)

    assert search_table.search(row) is True

def test_search_table_search_not_found(search_table: SearchTable):
    """Tests that a non-inserted row is not found."""
    row = {"id": 1, "category": "A"}

    assert search_table.search(row) is False


# --- Tests for rows_io ---

def test_write_and_read_rows(tmp_path: Path):
    """Tests that data can be written to and read from a .jsonl file."""
    identifier = "test_data"

    sample_data = [
        {"name": "Beren", "race": "Human"},
        {"name": "Luthien", "race": "Elf"},
    ]

    # Generator function for writing
    def generate_sample_data():
        yield from sample_data

    # 1. Write the data
    write_rows(tmp_path, identifier, generate_sample_data)

    # 2. Check that the file was created
    output_file = tmp_path / "test_data.jsonl"
    assert output_file.exists()

    # 3. Read the data back
    read_data = list(read_rows(tmp_path, identifier))

    # 4. Assert the data is the same
    assert len(read_data) == 2
    assert read_data == sample_data

def test_read_non_existent_file(tmp_path: Path):
    """Tests that read_rows handles non-existent files gracefully."""
    # Attempting to read should not raise an error and should yield no items.
    read_data = list(read_rows(tmp_path, "non_existent"))

    assert len(read_data) == 0

