

from soybean.utils import pinyin_translate

def test_translation():

    assert pinyin_translate("中文") == "ZhongWen"
    assert pinyin_translate("中文ABC") == "ZhongWenABC"

    s = "您好世界Hello World. 测试#102号"
    t = pinyin_translate(s)
    assert t == "NinHaoShiJieHello World. CeShi#102Hao"

    assert pinyin_translate("") == ""
     
