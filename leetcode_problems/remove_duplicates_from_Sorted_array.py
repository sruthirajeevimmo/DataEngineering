#Input: nums = [1,1,2]
#Output: 2, nums = [1,2,_]
def removeDuplicates(nums1):
    
    
    length=len(nums1)
    k=1
    nums1.sort()
    for i in nums1:
        if nums1[k-1]!=i:
            nums1[k]=i
            k+=1
            

    print(nums1)
    return k               
               
# Example usage
nums1 = [1, 1, 2]
k = removeDuplicates(nums1)
print(k)  # Output: 2
print(nums1)
print(nums1[:k])
