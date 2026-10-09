import os

def search_in_py_files(directory, search_term):
    """
    在指定目录下的所有.py文件中搜索包含特定内容的文件。
    
    :param directory: 需要搜索的根目录路径。
    :param search_term: 需要在文件中查找的字符串。
    """
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
            #    print(file)
            #if 1:
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        contents = f.read()
                        if search_term in contents:
                            print(f"Found '{search_term}' in file: {file_path}")
                except Exception as e:
                    #print(f"Could not read file {file_path}: {e}")
                    pass

if __name__ == "__main__":
    # 设置你想要搜索的目录和关键词
    search_directory = '/home/ubuntu/strategy_base'
    search_string = "F9D499D65932321247A23CE52ACF7A42"

    search_in_py_files(search_directory, search_string)
