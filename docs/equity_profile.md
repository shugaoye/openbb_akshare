Prompt:

```text
请设计一个模块化的数据缓存机制，用于缓存股票分析数据。主要缓存对象是 pandas DataFrame。要求使用 SQLite 数据库进行持久化存储。

目标缓存的数据为 EquityInfo 类型的 DataFrame，其主键字段为 `symbol`。

请完成以下任务：

1. 编写 Python 模块化代码，包含以下功能：
   - 建立与 SQLite 数据库的连接；
   - 创建表结构，字段如下（注意类型和空值支持）：
     - symbol: str (primary key)
     - org_name_en: Optional[str]
     - main_operation_business: Optional[str]
     - org_cn_introduction: Optional[str]
     - chairman: Optional[str]
     - org_website: Optional[str]
     - reg_address_cn: Optional[str]
     - office_address_cn: Optional[str]
     - telephone: Optional[str]
     - postcode: Optional[str]
     - provincial_name: Optional[str]
     - staff_num: Optional[int]
     - affiliate_industry: Optional[str]
     - operating_scope: Optional[str]
     - listed_date: Optional[date]
     - org_name_cn: Optional[str]
     - org_short_name_cn: Optional[str]
     - org_short_name_en: Optional[str]
     - org_id: Optional[str]
     - established_date: Optional[date]
     - actual_issue_vol: Optional[int]
     - reg_asset: Optional[float]
     - issue_price: Optional[float]
     - currency: Optional[str]

   - 实现将 DataFrame 写入数据库的方法；
   - 实现从数据库读取数据并返回 DataFrame 的方法；
   - 所有字段应正确映射到数据库列，并处理可能为空的情况；

2. 提供一个可以在 Markdown 文档中渲染的 Mermaid 格式 diagram，展示该缓存系统的模块结构或流程图。

3. 输出格式要求：
   - 先输出完整的 Python 代码，按模块化方式组织（如使用类封装）；
   - 然后输出 Mermaid 图表代码，用三个反引号包裹（```mermaid），方便 Markdown 渲染；
   - 不添加任何解释性文字，仅输出代码和图表。
```

测试数据：

