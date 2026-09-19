def split_list(arr, k):
    batches = [arr[i::k] for i in range(k)]
    return batches