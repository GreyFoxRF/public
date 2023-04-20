#Sudoku Solver
def sudoku(puzzle):
    import numpy as np
    arr = np.array(puzzle)
    def kill(i, j, arr):
        def cor(x):
            if x in [0,1,2]:
                return [0,3]
            elif x in [3, 4, 5]:
                return [3,6]
            else:
                return [6,9]
                
        l = [1,2,3,4,5,6,7,8,9]
        for x in arr[i]:
            try:
                l.remove(x)
            except:
                continue
        for x in arr[:,j]:
            try:
                l.remove(x)
            except:
                continue
        for x in arr[cor(i)[0]:cor(i)[1], cor(j)[0]: cor(j)[1]].reshape(9):
            
            try:
                l.remove(x)
            except:
                continue
        return l
    
    while 0 in arr:
        for i in range(9):
            for j in range(9):
                if arr[i,j] == 0:
                    l = kill(i,j,arr)
                    if len(l) == 1:
                        arr[i,j] = l[0]
    arr = list(map(list, arr))
    return arr