'''
在这个单元测试中,我们创建了一个测试类TestCountMarkPrice,其中包含了多个测试方法来覆盖不同的情况:

test_count_mark_price_positive_ratio:测试正比率的情况。
test_count_mark_price_negative_ratio:测试负比率的情况。
test_count_mark_price_zero_ratio:测试比率为 0 的情况。
test_count_mark_price_max_deviation:测试最大偏差的情况。
test_count_mark_price_re_mm_tatio:测试重新计算标记价格的情况。
在每个测试方法中,我们设置了相应的测试数据,并调用待测函数count_mark_price,然后将实际得到的标记价格与预期的标记价格进行比较,使用assertEqual断言它们是否相等。

通过运行这个单元测试,我们可以验证函数在各种情况下的正确性。
'''
import os
import sys
import unittest
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import single_mm as single

# 导入待测函数
def count_mark_price(self, ratio):
    # 函数实现代码
    pass

class TestCountMarkPrice(unittest.TestCase):

    def setUp(self):
        # 初始化测试数据
        self.obj = single()  # 创建待测类的实例

    def test_count_mark_price_positive_ratio(self):
        # 测试正比率情况
        ratio = 0.1
        expected_mark_price = 110  # 预期的标记价格
        actual_mark_price = self.obj.count_mark_price(ratio)
        self.assertEqual(actual_mark_price, expected_mark_price)

    def test_count_mark_price_negative_ratio(self):
        # 测试负比率情况
        ratio = -0.1
        expected_mark_price = 90  # 预期的标记价格
        actual_mark_price = self.obj.count_mark_price(ratio)
        self.assertEqual(actual_mark_price, expected_mark_price)

    def test_count_mark_price_zero_ratio(self):
        # 测试比率为 0 的情况
        ratio = 0
        expected_mark_price = 100  # 预期的标记价格
        actual_mark_price = self.obj.count_mark_price(ratio)
        self.assertEqual(actual_mark_price, expected_mark_price)

    def test_count_mark_price_max_deviation(self):
        # 测试最大偏差情况
        ratio = 0.2
        self.obj.net_pos_side = 'long'
        self.obj.pos_price = 110
        self.obj.max_deviation = 0.1
        expected_mark_price = 110  # 预期的标记价格
        actual_mark_price = self.obj.count_mark_price(ratio)
        self.assertEqual(actual_mark_price, expected_mark_price)

    def test_count_mark_price_re_mm_tatio(self):
        # 测试重新计算标记价格的情况
        ratio = 0.1
        self.obj.base_trade_price = 110
        self.obj.last_mark_price = 100
        self.obj.re_mm_tatio = 0.05
        expected_mark_price = 105  # 预期的标记价格
        actual_mark_price = self.obj.count_mark_price(ratio)
        self.assertEqual(actual_mark_price, expected_mark_price)

if __name__ == '__main__':
    unittest.main()

