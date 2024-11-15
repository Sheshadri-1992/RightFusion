
image_array = ['63KB.jpg', '86KB.jpg', '94KB.jpg', '150KB.jpg', '220KB.jpg', '336KB.jpg', '400KB.jpg', '500KB.jpg',
               '650KB.jpg', '800KB.jpg', '950KB.jpg', '1400KB.jpg', '1600KB.jpg', '1750KB.jpg', '1950KB.jpg',
               '2100KB.jpg']

num_images_array = [3, 1, 4, 1, 3, 1, 5, 3, 2, 5, 3, 2, 2, 4, 2, 3, 2, 2, 3, 2, 2, 1, 4, 1, 5, 1, 4, 5, 4, 3, 4, 2, 1,
                    1, 1, 4, 3, 2, 3, 4, 2, 4, 3, 5, 2, 2, 1, 2, 4, 3, 1, 4, 5, 4, 3, 2, 5, 3, 1, 4, 3, 1, 5, 1, 3, 1,
                    2, 3, 3, 1, 3, 5, 5, 5, 1, 2, 5, 2, 5, 5, 1, 1, 1, 2, 5, 2, 3, 4, 4, 3, 3, 1, 3, 4, 2, 1, 5, 2, 4,
                    4]
image_index_array = [8, 11, 12, 4, 5, 7, 15, 5, 8, 7, 8, 13, 3, 12, 0, 11, 8, 14, 7, 12, 8, 5, 1, 1, 7, 4, 5, 7, 8, 12,
                     0, 6, 14, 9, 10, 7, 8, 10, 12, 7, 12, 6, 8, 4, 8, 6, 5, 8, 7, 4, 8, 11, 7, 11, 4, 10, 6, 10, 15, 2,
                     7, 0, 5, 7, 7, 9, 8, 2, 4, 6, 8, 2, 6, 13, 12, 13, 1, 4, 4, 10, 6, 9, 6, 11, 7, 9, 8, 7, 11, 7, 3,
                     6, 12, 9, 6, 9, 10, 7, 13, 11]

total_size = 0

for i in range(0, len(image_index_array)):
    chosen_file = image_array[image_index_array[i]]
    file_size = float(chosen_file[:-6]) * num_images_array[i]
    total_size = total_size + file_size

print(total_size/1024)