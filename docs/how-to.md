---
layout: global
displayTitle: How To
title: How To
description: Wormhole How To page

---

* This will become a table of contents (this text will be scraped).
{:toc}
### DBus系统对接

Wormhole可直接消费DBus系统生成的UMS类型数据，具体配置步骤如下：

1. 修改wormhole application.conf文件中DBus服务接口配置项地址。

   ```
   vim application.conf

   #Dbus integration, if not set, please comment it
   dbus = {
     api = [
       {
         login = {
           url = "http://localhost:8080/keeper/login"
           email = ""
           password = ""
         }
         synchronization.namespace.url = "http://localhost:8080/keeper/tables/riderSearch"
       }
     ]
   }
   ```

   **注：只需修改ip和端口地址，根据部署的DBus服务调整，配置前可访问该地址测试下是否正确**。

2. Admin类型用户登录，刷新Namespace列表，会自动同步DBus服务目前已有的数据源Namespace信息。

   **注：Wormhole可直接消费DBus生成的UMS数据，无须配置源Namespace Schema信息**。

3. Admin类型用户为Project授权可访问的Namespace，可参考[Admin Guide](https://edp963.github.io/wormhole/admin-guide.html)章节。

4. User类型用户登录后新建Flow时Source DataSystem选择相应的系统类型即可。