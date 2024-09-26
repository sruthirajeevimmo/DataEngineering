#Input: nums = [1,1,2]
#Output: 2, nums = [1,2,_]
def removeDuplicates(nums):
    expected_num=[]
    for i in range(len(nums)):

        if nums[i] not in expected_num:
            expected_num.append(nums[i])
            if nums.count(nums[i])!=2:
                 expected_num.append(nums[i])
        print(expected_num)
        return len(expected_num)

               
# Example usage
nums = [1,1,1,2,2,3] 
expectedNums = [...] 

k = removeDuplicates(nums)
print(k)