#Input: nums1 = [1,2,3,0,0,0], m = 3, nums2 = [2,5,6], n = 3
#Output: [1,2,2,3,5,6]

def merge(nums1, m, nums2, n):
    length = m+n
    for i in range(m, length):
        nums1[i] = nums2[i-m]
    nums1.sort()
    return nums1
nums1=merge([1,2,3,0,0,0], 3, [2,5,6], 3)
print(nums1)