import unicodedata

def standard_location(district: str):
    """Lowercase and remove tone mark"""
    d_name = district.lower().replace(" ", "-").replace("quận", "district")
    d_name = unicodedata.normalize('NFD', d_name)
    d_name = ''.join(c for c in d_name if unicodedata.category(c) != 'Mn')
    d_name = d_name.replace('đ', 'd').replace('Đ', 'D')
    d_name = d_name.replace("quan", "district")
    return d_name

if __name__ == "__main__":
    districts = ["Huyện Bình Chánh", "Quận 11", "quan 10", "huyen bình Chánh", "Thủ đức", "thu dau mot"]
    for d in districts:
        print(standard_location(d))