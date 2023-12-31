from typing import List, Optional, TypeVar

TItem = TypeVar("TItem")


def bs_has(item: TItem, sorted_items: List[TItem]) -> bool:
    """
    Check if an element is present in a sorted list.

    :param item: The element to search for.
    :param sorted_items: A sorted list of items.

    :return: True if the element is present, False otherwise.
    """
    if len(sorted_items) == 0 or item < sorted_items[0] or sorted_items[-1] < item:
        return False
    left_index, right_index = 0, len(sorted_items) - 1
    while left_index < right_index:
        mid_index = (left_index + right_index + 1) // 2
        if item < sorted_items[mid_index]:
            right_index = mid_index - 1
        else:
            left_index = mid_index
    return sorted_items[left_index] == item


def bs_lower(item: TItem, sorted_items: List[TItem]) -> Optional[int]:
    """
    Find index of the biggest element of a sorted list that is not greater than a specified element.

    :param item: The specified element.
    :param sorted_items: A sorted list of items.

    :return: The index if found, None if all items are greater than the specified element.
    """
    if len(sorted_items) == 0 or item < sorted_items[0]:
        return None
    left_index, right_index = 0, len(sorted_items) - 1
    while left_index < right_index:
        mid_index = (left_index + right_index + 1) // 2
        if item < sorted_items[mid_index]:
            right_index = mid_index - 1
        else:
            left_index = mid_index
    return left_index
