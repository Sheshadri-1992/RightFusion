from PIL import Image
import cv2

image_path = "shawshank.jpg"
img = Image.open(image_path)
print(img.size)
# new_image = img.resize((4000, 2400))
# new_image.save("2100KB.jpg")
# print(new_image.size)
# img.save("pilimage.jpg", 'JPEG')

# img = cv2.imread(image_path)
# cv2.imwrite("cv2image.jpg", img, [int(cv2.IMWRITE_JPEG_QUALITY), 75])
