#Input: nums = [3,2,2,3], val = 3
#Output: 2, nums = [2,2,_,_]
def remove_element(nums, val):
    i=0
    while i < len(nums):
        if nums[i]==val:
            nums.pop(i)
        else:
            i+=1
    return nums
nums1=remove_element([3,2,2,3],3)
print(nums1)