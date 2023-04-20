def smaller(arr):
    import numpy as np
    l = np.zeros(max(arr) + 1, dtype = np.int64)
    lm = np.zeros(abs(min(arr)) + 1, dtype = np.int64)
    arr.reverse()
    for i in range(len(arr)):
        if arr[i] >= 0:
            l[arr[i]] += 1
            arr[i] = l[:arr[i]].sum() + lm.sum()
        else:
            lm[abs(arr[i])] += 1
            arr[i] = lm[abs(arr[i])+1:].sum()
    arr.reverse()
    return arr