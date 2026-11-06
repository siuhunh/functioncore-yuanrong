## 				脚本说明

1.使用说明

- 脚本功能：统计functiontask目录中go接口的测试覆盖率，
- 使用方式：直接调用，如``` sh test.sh```

2.当前脚本中对每个目录都进行了统计覆盖率的操作，但没有测试脚本的接口不被统计，因此当前统计的结果只包括：有测试脚本的那部分接口。

- 没有测试脚本的目录，运行脚本时会有提示，如

```?       huawei.com/snc/functiontask/adaptor     [no test files]```

3.当前每个目录下的单元测试覆盖率都是使用硬编码语句统计，后续添加或删除目录时，需要同步修改对应的内容。如

```shell
COVERAGEPATH="${GOBUILD_DIR}/functiontask/util" #指定目录
echo $COVERAGEPATH
cd $COVERAGEPATH  #进入该目录

#统计当前目录下 short目录中代码的测试覆盖率，并将结果输出到'$CUR_DIR"/util-short.cov'文件中
if ! CGO_ENABLED=1 GOOS=linux go test -v -cover -coverprofile="$CUR_DIR"/util-short.cov ./short; then
    hares_log_error "go test util short failed"
    exit 1
fi

```



4.为了后续开发人员方便为接口添加测试脚本，后面的内容会简单描述统计单元测试覆盖率的步骤。

① **背景简介**

​	Go语言自带对单元测试和代码覆盖率统计的支持，相比之前的各种语言（C、Java等等），单元测试代码编写方便，测试代码可以完全独立于程序源码，不需要对代码插桩，只需少量地修改代码的编译指令。因此非常方便实用。

​	下面介绍Go语言对代码覆盖率统计支持的关键命令：`go test`。

② **go test**

​	go test命令是一个按照一定的约定和组织来测试代码的程序。在包目录内，所有以_test.go为后缀名的源文件在执行go build时不会被构建成包的一部分，它们是go test测试的一部分。

​	在`*_test.go`文件中，我们可以使用测试函数。一个测试函数是以Test为函数名前缀的函数，用于测试程序的一些逻辑行为是否正确；`go test`命令会调用这些测试函数并报告测试结果是PASS或FAIL。

③ **实例**

1.编写一个简单的go程序名为"calc.go":

```
package main
func Sub(a, b int) int{
      var res int
      if (a > b) {
              res = a - b
      } else {
              res = b - a
      }
      return res
}
```

2.为`calc.go`编写一个测试程序，名为`calc_test.go"`。测试代码有以下要求：

-  测试代码的文件必须以“_test.go”作为文件名的结尾。
-  测试代码必须引入testing包。
-  测试代码中的测试函数必须以“Test”作为函数名前缀，且参数值要是testing.T。


```
package main

import (
    "testing"
        "fmt"
)
var subTest = []struct{
        a int
        b int
        prt int
}{
{10, 12, 2},
//{15, 14, 1},
}

func TestSub(t *testing.T) {
        for _, val := range subTest {
                if Sub(val.a, val.b)!= val.prt{
                        fmt.Println(("实际值与期望值不符!"))
                }
        }
}

```

3.测试代码写好后，可以运行go test，查看到测试结果：

```
huangyongbo@ubuntu151:~/work_space/go/readme$ ls
calc.go  calc_test.go
huangyongbo@ubuntu151:~/work_space/go/readme$ go test
PASS
ok      sum/readme      0.002s
```

4.要测试代码覆盖率的话，需要在go test上增加参数，变成：

   `go test –coverprofile=cover.out`

   再次执行，可看到这次会打印代码覆盖率了，并在当前目录下生成统计结果的文件cover.out

   ```
   PASS
   coverage: 80.0% of statements
   ok      sum/readme      0.002s
   huangyongbo@ubuntu151:~/work_space/go/readme$ ls
   calc.go  calc_test.go  cover.out
   ```

 

5.内容可视化及其内容含义

- 将结果可视化，可以通过以下操作：

  ` gocov convert out.cov > out.json`

  `gocov-html out.json > out.html`

  *安装命令：`go get github.com/axw/gocov/gocov` ;`go get github.com/matm/gocov-html `*

- 内容简析

  打开out.html可以看见代码覆盖率的结果。结果部分如下表：

  | `Sub(...)` | `coverage/calc.go` | `80.00%` | `4/5` |
  | ---------- | ------------------ | -------- | ----- |
  | `coverage` |                    | `80.00%` | `4/5` |

  最后一行为结果汇总。

  第一行代表着Sub接口的代码测试覆盖率为80%，接口中一共有5个语句，有4个语句被执行了。以下在代码中标出具体语句：

    ```
  func Sub(a, b int) int{
      var res int  //语句1
      if (a > b) { //语句2
              res = a - b   //语句3
      } else {
              res = b - a   //语句4
      }
      return res          //语句5
  }
    ```

  由于测试上述例子中的输入用例为：

  ```
  var subTest = []struct{
        a int
        b int
        prt int
  }{
  {10, 12, 2},
  //{15, 14, 1},
  }
  ```

  因此测试用例运行时，不会执行语句3。（在out.html中没有执行的语句会被标红）

- 提高测试覆盖率

  由上述内容可知，覆盖率 = 测试时执行的语句数 / 接口总语句数。所以只需要修改用例，使接口中的语句尽可能都执行一次，就可以提高覆盖率。

  还是使用"calc.go"，将“calc_test.go"中的注释去掉，改为

  ```
  var subTest = []struct{
        a int
        b int
        prt int
  }{
  {10, 12, 2},
  {15, 14, 1},
  }
  ```

  然后再测试覆盖率，此时得到的覆盖率为100%
