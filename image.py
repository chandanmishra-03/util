from PIL import Image
from io import BytesIO  

filename = "test.jpg"
pil_image = Image.open(filename)
width, height = pil_image.size  
print("width",width )
print("height",height )
r_width = 400
r_height = int(r_width*((height/width)))

pil_image = pil_image.resize((width, height), Image.ANTIALIAS)

pil_image_rgb = pil_image.convert('RGB')
pil_image_rgb.save("res.jpg", format='JPEG', quality=90)
