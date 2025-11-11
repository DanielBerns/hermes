import bisect
from typing import List, Tuple

def search_insertion_context(
    reference: List[str],
    candidate: str
) -> Tuple[str | None, str | None]:
    """
    Find the insertion point of candidate in the
    sorted reference list and returns the element, along with the elements
    immediately before and after the insertion point.

    This function leverages the `bisect` module for efficient O(log n) lookups,
    making it suitable for very large lists.

    Args:
        reference: A sorted list of strings where context will be found.
        candidate: A list of strings to find context for.

    Returns:
        A tuple contains two elements:
        - The element from the reference list just before the insertion point (or None).
        - The element from the reference list at the insertion point (or None).
    """
    # Find the insertion point using binary search.
    # This is the index where 'item' would be inserted.
    ip = bisect.bisect_left(reference, candidate)

    # Determine the element before the insertion point, handling the edge case
    # where the insertion point is at the beginning of the list.
    before = reference[ip - 1] if ip > 0 else None

    # Determine the element after the insertion point, handling the edge case
    # where the insertion point is at the end of the list.
    after = reference[ip] if ip < len(reference) else None

    return before, after

def compare_prefix(red: str, blue: str) -> int:
    idx = 0
    for rr, bb in zip(red, blue):
        if rr == bb:
            idx += 1
        else:
            break
    return idx

def levenshtein(s1: str, s2: str) -> int:
    """
    Calculates the Levenshtein distance between two strings using dynamic programming.
    This version is optimized for space, using O(min(len(s1), len(s2))) space.

    The Levenshtein distance is the minimum number of single-character edits
    (insertions, deletions or substitutions) required to change one word into the other.

    Args:
        s1: The first string.
        s2: The second string.

    Returns:
        The Levenshtein distance between the two strings.
    """
    m, n = len(s1), len(s2)

    # To optimize space, we want the outer loop to be over the longer string
    # and our DP rows to be the size of the shorter string.
    if m < n:
        s1, s2 = s2, s1
        m, n = n, m

    # We only need to keep track of the previous row to compute the current row.
    # `prev_row` is initialized to the distance from an empty string.
    prev_row = list(range(n + 1))

    # Fill in the conceptual matrix row by row
    for i in range(1, m + 1):
        # `current_row` will be built based on `prev_row`.
        # The first element of the current row is `i` (deletions from s1).
        current_row = [i] * (n + 1)
        for j in range(1, n + 1):
            # If the characters are the same, the cost is 0, otherwise it's 1
            cost = 0 if s1[i - 1] == s2[j - 1] else 1

            # The value is the minimum of three operations:
            deletion = prev_row[j] + 1
            insertion = current_row[j - 1] + 1
            substitution = prev_row[j - 1] + cost
            current_row[j] = min(deletion, insertion, substitution)

        # The current row becomes the previous row for the next iteration
        prev_row = current_row

    # The final distance is the last element of the last computed row
    return prev_row[n]

