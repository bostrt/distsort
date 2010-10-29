def merge_sort(list):
    if len(list) < 2:
        return list
    
    leftHalf  = list[0:len(list)/2]
    rightHalf = list[len(list)/2:len(list)]
    
    left  = merge_sort(leftHalf)
    right = merge_sort(rightHalf)
    
    result = merge(left, right)
    return result

def merge(left, right):
    result = []
    # Loop over lists while both still have elements
    while len(left) > 0 or len(right) > 0:
        if len(left) > 0 and len(right) > 0:
            if left[0] >= right[0]:
                result.append(right[0])
                # remove first element from righnt list
                right = right[1:]
            else:
                result.append(left[0])
                # remove first element from left list
                left = left[1:]
        elif len(left) > 0:
            [result.append(x) for x in left]
            break
        elif len(right) > 0:
            [result.append(x) for x in right]
            break
    return result
