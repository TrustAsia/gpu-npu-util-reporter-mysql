//! # expr 模块
//!
//! 轻量表达式求值器（纯函数，无副作用，无 I/O）。
//!
//! 仅支持 `+ - * / ()` 四则运算、一元负号、括号，以及变量名。用于配置中
//! `expressions` 的派生指标计算，例如显存占用率 `USED / (USED + FREE)`。
//!
//! ## 设计目标
//! - **纯函数**：相同输入恒定输出，无状态、无 I/O，极易测试。
//! - **变量名 = metric 名**：表达式里的每个变量都对应一个 Prometheus metric 名，
//!   求值时由调用方提供 `变量名 -> 值` 的映射（来自该卡的对齐样本）。
//! - **错误分层**：语法错误（括号不匹配、非法字符等）在 [`parse`] 阶段即返回
//!   [`ParseError`]，配置加载时拦截，导致启动失败；运行时的除零、变量缺失在
//!   [`evaluate`] 阶段返回 `None`，只让该派生字段写 NULL，不污染整行。
//!
//! ## 算法
//! 经典递归下降解析。文法（运算优先级自下而上递增）：
//! ```text
//! expr   := term (('+' | '-') term)*        // 加减，左结合
//! term   := factor (('*' | '/') factor)*    // 乘除，左结合
//! factor := number                          // 数值字面量
//!         | variable                         // 变量(=metric 名)
//!         | '(' expr ')'                     // 括号分组
//!         | '-' factor                       // 一元负号
//! ```
//! 变量名匹配 `[A-Za-z_][A-Za-z0-9_]*`（与 Prometheus metric 名规则一致，
//! 如 `DCGM_FI_DEV_FB_USED`）。

use std::collections::HashMap;

/// 解析后的表达式抽象语法树（内部类型，仅本模块使用）。
///
/// 故意保持私有：调用方只通过 [`parse`] 得到它、再交给 [`evaluate`] 求值，
/// 不应也不必在外部遍历其结构。
#[derive(Debug, Clone)]
enum Ast {
    /// 数值字面量。
    Num(f64),
    /// 变量引用，存变量名（= metric 名）。
    Var(String),
    /// 一元负号，如 `-A`。
    Neg(Box<Ast>),
    /// 二元运算：运算符 + 左子树 + 右子树。
    BinOp(Op, Box<Ast>, Box<Ast>),
}

/// 二元运算符种类。
#[derive(Debug, Clone, Copy)]
enum Op {
    Add,
    Sub,
    Mul,
    Div,
}

/// 解析错误（语法层面）。
///
/// 仅携带可读的错误描述字符串。配置校验阶段据此向用户报告哪条表达式、
/// 何处的语法问题导致启动失败。
#[derive(Debug, Clone, PartialEq)]
pub struct ParseError(pub String);

/// 解析表达式字符串为 AST。变量名即 metric 名。
///
/// 在配置加载阶段调用：任何语法错误都应导致程序启动失败（配置是确定性输入，
/// 不应在运行期才暴露）。解析成功后整个输入必须被消费完，否则视为尾部有
/// 意外字符（例如 `A B`）而报错。
///
/// # 参数
/// - `input`: 表达式原文，如 `"DCGM_FI_DEV_FB_USED / (USED + FREE)"`。
///
/// # 返回
/// 成功返回 AST；失败返回 [`ParseError`]。
///
/// 注：返回的 [`Ast`] 类型本身对外不透明（私有），调用方通过类型推断
/// 接住它再交给 [`evaluate`]，不直接命名其类型。此处对编译器告警
/// 显式静默：这是有意的"私有类型、公有函数"设计。
#[allow(private_interfaces)]
pub fn parse(input: &str) -> Result<Ast, ParseError> {
    let mut p = Parser {
        chars: input.chars().peekable(),
    };
    let ast = p.parse_expr()?;
    p.skip_ws();
    // 解析完整表达式后，必须已读到结尾；残留字符说明输入不合法（如 "A B"）。
    if p.chars.peek().is_some() {
        return Err(ParseError(format!("表达式末尾有未识别字符: '{}'", p.rest_preview())));
    }
    Ok(ast)
}

/// 递归下降解析器。逐字符消费输入，通过 `peek` 做向前看一字符的判断。
struct Parser<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
}

impl<'a> Parser<'a> {
    /// 跳过空白字符（空格、制表符、换行等）。
    fn skip_ws(&mut self) {
        while let Some(&c) = self.chars.peek() {
            if c.is_whitespace() {
                self.chars.next();
            } else {
                break;
            }
        }
    }

    /// 为错误信息提供一个剩余输入的预览（尽力而为，非完整剩余串）。
    fn rest_preview(&mut self) -> String {
        let mut s = String::new();
        for _ in 0..16 {
            match self.chars.next() {
                Some(c) => s.push(c),
                None => break,
            }
        }
        s
    }

    /// expr := term (('+' | '-') term)*
    fn parse_expr(&mut self) -> Result<Ast, ParseError> {
        let mut left = self.parse_term()?;
        loop {
            self.skip_ws();
            match self.chars.peek() {
                Some('+') => {
                    self.chars.next();
                    let right = self.parse_term()?;
                    left = Ast::BinOp(Op::Add, Box::new(left), Box::new(right));
                }
                Some('-') => {
                    self.chars.next();
                    let right = self.parse_term()?;
                    left = Ast::BinOp(Op::Sub, Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    /// term := factor (('*' | '/') factor)*
    fn parse_term(&mut self) -> Result<Ast, ParseError> {
        let mut left = self.parse_factor()?;
        loop {
            self.skip_ws();
            match self.chars.peek() {
                Some('*') => {
                    self.chars.next();
                    let right = self.parse_factor()?;
                    left = Ast::BinOp(Op::Mul, Box::new(left), Box::new(right));
                }
                Some('/') => {
                    self.chars.next();
                    let right = self.parse_factor()?;
                    left = Ast::BinOp(Op::Div, Box::new(left), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    /// factor := number | variable | '(' expr ')' | '-' factor
    fn parse_factor(&mut self) -> Result<Ast, ParseError> {
        self.skip_ws();
        match self.chars.peek() {
            // 括号分组：递归解析内部 expr，并要求配对的右括号。
            Some('(') => {
                self.chars.next(); // 消费 '('
                let inner = self.parse_expr()?;
                self.skip_ws();
                match self.chars.next() {
                    Some(')') => Ok(inner),
                    _ => Err(ParseError("缺少右括号 ')'".into())),
                }
            }
            // 一元负号：递归解析因子并取反。
            Some('-') => {
                self.chars.next(); // 消费 '-'
                let inner = self.parse_factor()?;
                Ok(Ast::Neg(Box::new(inner)))
            }
            // 数值字面量：以数字或小数点开头。
            Some(c) if c.is_ascii_digit() || *c == '.' => self.parse_number(),
            // 变量：以 ASCII 字母或下划线开头（严格匹配文法 [A-Za-z_]）。
            Some(c) if c.is_ascii_alphabetic() || *c == '_' => self.parse_var(),
            other => Err(ParseError(format!("意外字符: {:?}", other))),
        }
    }

    /// 解析数值字面量（连续的数字与小数点），转为 f64。
    fn parse_number(&mut self) -> Result<Ast, ParseError> {
        let mut s = String::new();
        while let Some(&c) = self.chars.peek() {
            if c.is_ascii_digit() || c == '.' {
                s.push(c);
                self.chars.next();
            } else {
                break;
            }
        }
        s.parse::<f64>()
            .map(Ast::Num)
            .map_err(|_| ParseError(format!("非法数字: {}", s)))
    }

    /// 解析变量名（[A-Za-z_][A-Za-z0-9_]*），即 metric 名。
    fn parse_var(&mut self) -> Result<Ast, ParseError> {
        let mut s = String::new();
        while let Some(&c) = self.chars.peek() {
            if c.is_ascii_alphanumeric() || c == '_' {
                s.push(c);
                self.chars.next();
            } else {
                break;
            }
        }
        Ok(Ast::Var(s))
    }
}

/// 对 AST 求值。变量值由 `vars` 提供（key = metric 名，value = 该卡的值）。
///
/// # 返回
/// - 正常情况返回 `Some(值)`。
/// - 遇到 **除零** 或 **变量缺失**（该卡没有该 metric）返回 `None`：
///   调用方据此把该派生字段写 NULL，而不是让整条采集失败。
///
/// 这种"软失败"是有意为之：单张卡缺某个 metric 不应导致整个 source
/// 本轮所有卡都丢失数据。
pub fn evaluate(ast: &Ast, vars: &HashMap<String, f64>) -> Option<f64> {
    match ast {
        Ast::Num(n) => Some(*n),
        Ast::Var(name) => vars.get(name).copied(),
        Ast::Neg(inner) => evaluate(inner, vars).map(|v| -v),
        Ast::BinOp(op, l, r) => {
            // 任一子树为 None（缺变量/除零）则整体 None，向上传播。
            let lv = evaluate(l, vars)?;
            let rv = evaluate(r, vars)?;
            match op {
                Op::Add => Some(lv + rv),
                Op::Sub => Some(lv - rv),
                Op::Mul => Some(lv * rv),
                Op::Div => {
                    if rv == 0.0 {
                        None
                    } else {
                        Some(lv / rv)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试辅助：把 `&[(&str, f64)]` 转成求值所需的变量表。
    fn vars(pairs: &[(&str, f64)]) -> HashMap<String, f64> {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn basic_division() {
        let ast = parse("A / B").unwrap();
        assert_eq!(evaluate(&ast, &vars(&[("A", 6.0), ("B", 3.0)])), Some(2.0));
    }

    #[test]
    fn paren_and_three_vars() {
        let ast = parse("(A + B) / C").unwrap();
        assert_eq!(
            evaluate(&ast, &vars(&[("A", 1.0), ("B", 2.0), ("C", 3.0)])),
            Some(1.0)
        );
    }

    #[test]
    fn division_by_zero_returns_none() {
        let ast = parse("A / B").unwrap();
        assert_eq!(evaluate(&ast, &vars(&[("A", 1.0), ("B", 0.0)])), None);
    }

    #[test]
    fn missing_variable_returns_none() {
        let ast = parse("A / B").unwrap();
        assert_eq!(evaluate(&ast, &vars(&[("A", 1.0)])), None);
    }

    #[test]
    fn syntax_error_incomplete() {
        assert!(parse("A /").is_err());
    }

    #[test]
    fn metric_name_with_underscores() {
        // 真实 metric 名如 DCGM_FI_DEV_FB_USED，必须能作为变量名解析。
        let ast = parse("DCGM_FI_DEV_FB_USED / DCGM_FI_DEV_FB_FREE").unwrap();
        let v = vars(&[("DCGM_FI_DEV_FB_USED", 4.0), ("DCGM_FI_DEV_FB_FREE", 4.0)]);
        assert_eq!(evaluate(&ast, &v), Some(1.0));
    }

    #[test]
    fn missing_closing_paren() {
        assert!(parse("(A + B").is_err());
    }

    #[test]
    fn unary_negation() {
        let ast = parse("-A + B").unwrap();
        assert_eq!(evaluate(&ast, &vars(&[("A", 1.0), ("B", 3.0)])), Some(2.0));
    }
}
