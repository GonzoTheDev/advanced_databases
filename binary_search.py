def binary_search(arr, target):
    """
    Binary search algorithm
    """
    low = 0
    high = len(arr) - 1
    while low <= high:
        mid = (low+high)//2
        if arr[mid] == target:
            return mid
        elif arr[mid] > target:
            high = mid - 1
        else:
            low = mid + 1
    return -1

myarray = [1, 2, 3, 4, 5, 6, 7, 8, 9]
print(binary_search(myarray, 1))
