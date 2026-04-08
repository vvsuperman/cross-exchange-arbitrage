#!/usr/bin/env python3
"""检查 .env 文件配置是否正确"""
import os
from dotenv import load_dotenv

def check_env():
    """检查环境变量配置"""
    print("=" * 60)
    print("检查 .env 文件配置")
    print("=" * 60)
    
    # 加载 .env 文件
    result = load_dotenv()
    if not result:
        print("⚠️  警告: 无法加载 .env 文件，请确认文件存在")
        return
    
    print("✅ .env 文件已加载\n")
    
    # 检查必需的环境变量
    required_vars = {
        'LIGHTER_ACCOUNT_INDEX': '整数',
        'LIGHTER_API_KEY_INDEX': '整数',
        'EDGEX_ACCOUNT_ID': '字符串',
        'EDGEX_STARK_PRIVATE_KEY': '字符串',
        'API_KEY_PRIVATE_KEY': '字符串',
    }
    
    optional_vars = {
        'EDGEX_BASE_URL': '字符串（可选，有默认值）',
        'EDGEX_WS_URL': '字符串（可选，有默认值）',
    }
    
    errors = []
    warnings = []
    
    print("必需的环境变量:")
    print("-" * 60)
    for var_name, var_type in required_vars.items():
        value = os.getenv(var_name)
        if value is None:
            errors.append(f"❌ {var_name}: 未设置")
            print(f"❌ {var_name}: 未设置")
        elif var_type == '整数':
            if value.startswith('your_') or value.startswith('YOUR_'):
                errors.append(f"❌ {var_name}: 仍是占位符值 '{value}'，请替换为实际数值")
                print(f"❌ {var_name}: 仍是占位符值 '{value}'，请替换为实际数值")
            else:
                try:
                    int_value = int(value)
                    print(f"✅ {var_name}: {int_value} (类型: {var_type})")
                except ValueError:
                    errors.append(f"❌ {var_name}: '{value}' 不是有效的整数")
                    print(f"❌ {var_name}: '{value}' 不是有效的整数")
        else:  # 字符串
            if value.startswith('your_') or value.startswith('YOUR_'):
                errors.append(f"❌ {var_name}: 仍是占位符值 '{value}'，请替换为实际值")
                print(f"❌ {var_name}: 仍是占位符值 '{value}'，请替换为实际值")
            elif len(value) == 0:
                errors.append(f"❌ {var_name}: 值为空")
                print(f"❌ {var_name}: 值为空")
            else:
                masked_value = value[:10] + "..." if len(value) > 10 else value
                print(f"✅ {var_name}: {masked_value} (长度: {len(value)})")
    
    print("\n可选的环境变量:")
    print("-" * 60)
    for var_name, var_type in optional_vars.items():
        value = os.getenv(var_name)
        if value:
            print(f"✅ {var_name}: {value}")
        else:
            print(f"⚪ {var_name}: 未设置（将使用默认值）")
    
    print("\n" + "=" * 60)
    if errors:
        print("❌ 发现以下问题:")
        for error in errors:
            print(f"   {error}")
        print("\n请修复这些问题后重新运行程序。")
        return False
    else:
        print("✅ 所有必需的环境变量都已正确配置！")
        return True

if __name__ == "__main__":
    success = check_env()
    exit(0 if success else 1)

