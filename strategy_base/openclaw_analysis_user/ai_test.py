import google.generativeai as genai
import os

# 1. 配置你的 API Key
# 建议：在终端执行 export GOOGLE_API_KEY='你的KEY' 
# 或者直接在下方字符串中填入（仅限测试，注意安全）
#os.environ["GOOGLE_API_KEY"] = "AIzaSyBvJJUIqoQ93CZCOUSwMRcOLBt6FmWkWF8"

#genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

# 2. 初始化模型 (推荐使用 1.5-flash，速度最快)
#model = genai.GenerativeModel('models/gemini-flash-lite-latest')

# 3. 发送请求并打印结果
#try:
#    response = model.generate_content("你好 Gemini，请确认你已连接成功。")
#    print(response.text)
#except Exception as e:
#    print(f"调用出错: {e}")


# 以下代码查看可用模型
import google.generativeai as genai
import os

os.environ["GOOGLE_API_KEY"] = "AIzaSyBvJJUIqoQ93CZCOUSwMRcOLBt6FmWkWF8"
genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

# # 打印所有可用的模型名称，方便排查
print("--- 你账号支持的模型列表 ---")
for m in genai.list_models():
    if 'generateContent' in m.supported_generation_methods:
        print(m.name)
print("---------------------------\n")

# # 使用 list_models 中显示的完整名称
# try:
#     # 2026年通常直接使用这个别名即可
#     model = genai.GenerativeModel('models/gemini-1.5-flash')
#     response = model.generate_content("Testing.")
#     print(f"成功调用！反馈内容：{response.text}")
# except Exception as e:
#     print(f"再次报错: {e}")

