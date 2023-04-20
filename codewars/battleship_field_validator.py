def validate_battlefield(field):
    import numpy as np
    if np.array(field).sum() != 20:
        return False
    arr = np.array(field, dtype=str)
    d = {'0110': 3, '01110': 2, '011110': 1}
    for kk in range(2):
        for ii in range(len(arr)):
            for jj in range(len(arr[ii])):
                if arr[ii,jj] == '1':
                    m = arr[max(0, ii - 1) : min(np.shape(arr)[0], ii + 2), max(0, jj - 1) : min(np.shape(arr)[1], jj + 2)]
                    if list(np.diag(m, k=0)).count('1') > 1:
                        return False
                    

            i ='0' + ''.join(list(arr[ii])) + '0'
            for j in d:
                d[j] -= i.count(j)
                if d[j] < 0:
                    print(i)
                    print(d)
                    return False
        arr = np.rot90(arr)
    return True