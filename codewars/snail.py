def snail(snail):
    import numpy as np
    l = []
    arr = np.array(snail)
    while len(arr) > 0:
        l += arr[0].tolist()
        arr = np.rot90(arr[1:])
    return l