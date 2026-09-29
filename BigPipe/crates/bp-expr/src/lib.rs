//! **bpql** — BigPipe's small expression language.
//!
//! Used for server-side consumer filters (`header("region") == "ID" && this.amount > 1000`),
//! share-group filters and flow mappings (`root.total = this.qty * this.price`).
//! Expressions are parsed once into an AST and evaluated per record without allocation for
//! header/key lookups. The record value is parsed as JSON lazily, only when `this` is used.
//!
//! Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.

use std::cell::OnceCell;

use serde_json::{Map, Number, Value};

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum ExprError {
    #[error("parse error at {pos}: {msg}")]
    Parse { pos: usize, msg: String },
    #[error("unknown function `{0}`")]
    UnknownFunction(String),
    #[error("function `{name}` expects {expected} argument(s)")]
    Arity { name: String, expected: &'static str },
}

/// The record an expression is evaluated against.
pub trait RecordContext {
    fn key(&self) -> Option<&[u8]>;
    fn value(&self) -> Option<&[u8]>;
    fn header(&self, name: &str) -> Option<&[u8]>;
    fn topic(&self) -> &str {
        ""
    }
    fn partition(&self) -> i32 {
        0
    }
    fn offset(&self) -> i64 {
        -1
    }
    fn timestamp(&self) -> i64 {
        -1
    }
}

/// Simple owned record, handy for tests and non-Kafka callers.
#[derive(Debug, Default, Clone)]
pub struct SimpleRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

impl RecordContext for SimpleRecord {
    fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }
    fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
    fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_slice())
    }
    fn topic(&self) -> &str {
        &self.topic
    }
    fn partition(&self) -> i32 {
        self.partition
    }
    fn offset(&self) -> i64 {
        self.offset
    }
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

// ---------------------------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(f64),
    Str(String),
    Ident(String),
    Op(&'static str),
    Eof,
}

fn lex(src: &str) -> Result<Vec<(Tok, usize)>, ExprError> {
    let b = src.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    let err = |pos: usize, msg: &str| ExprError::Parse { pos, msg: msg.to_string() };
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let start = i;
        if c.is_ascii_digit() {
            while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.' || b[i] == b'_' || b[i] == b'e' || b[i] == b'E') {
                i += 1;
            }
            let s: String = src[start..i].chars().filter(|c| *c != '_').collect();
            let n = s.parse::<f64>().map_err(|_| err(start, "invalid number"))?;
            out.push((Tok::Num(n), start));
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_') {
                i += 1;
            }
            out.push((Tok::Ident(src[start..i].to_string()), start));
            continue;
        }
        if c == b'"' || c == b'\'' {
            let quote = c;
            i += 1;
            let mut s = String::new();
            loop {
                if i >= b.len() {
                    return Err(err(start, "unterminated string"));
                }
                let ch = src[i..].chars().next().unwrap();
                if ch as u32 == quote as u32 {
                    i += 1;
                    break;
                }
                if ch == '\\' {
                    i += 1;
                    let esc = src[i..].chars().next().ok_or_else(|| err(i, "bad escape"))?;
                    s.push(match esc {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        other => other,
                    });
                    i += esc.len_utf8();
                    continue;
                }
                s.push(ch);
                i += ch.len_utf8();
            }
            out.push((Tok::Str(s), start));
            continue;
        }
        const OPS: [&str; 22] = [
            "==", "!=", "<=", ">=", "&&", "||", "<", ">", "!", "+", "-", "*", "/", "%", "(", ")", "[", "]", ",", ".", "=", ";",
        ];
        let op = OPS.iter().find(|op| src[i..].starts_with(**op)).ok_or_else(|| err(i, "unexpected character"))?;
        i += op.len();
        out.push((Tok::Op(op), start));
    }
    out.push((Tok::Eof, src.len()));
    Ok(out)
}

// ---------------------------------------------------------------------------------------------
// AST + parser
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
enum BinOp {
    Or,
    And,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Contains,
    StartsWith,
    EndsWith,
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Func {
    Header,
    Key,
    Value,
    Json,
    Topic,
    Partition,
    Offset,
    Timestamp,
    Now,
    Lower,
    Upper,
    Len,
    Contains,
    StartsWith,
    EndsWith,
    Number,
    String,
    Exists,
    Abs,
    Round,
    Floor,
    Ceil,
    Coalesce,
    Deleted,
    Uuid,
}

impl Func {
    fn lookup(name: &str) -> Option<(Func, usize, usize)> {
        Some(match name {
            "header" => (Func::Header, 1, 1),
            "key" => (Func::Key, 0, 0),
            "value" | "content" => (Func::Value, 0, 0),
            "json" => (Func::Json, 0, 1),
            "topic" => (Func::Topic, 0, 0),
            "partition" => (Func::Partition, 0, 0),
            "offset" => (Func::Offset, 0, 0),
            "timestamp" => (Func::Timestamp, 0, 0),
            "now" => (Func::Now, 0, 0),
            "lower" => (Func::Lower, 1, 1),
            "upper" => (Func::Upper, 1, 1),
            "len" | "length" => (Func::Len, 1, 1),
            "contains" => (Func::Contains, 2, 2),
            "starts_with" | "startsWith" => (Func::StartsWith, 2, 2),
            "ends_with" | "endsWith" => (Func::EndsWith, 2, 2),
            "number" => (Func::Number, 1, 1),
            "string" => (Func::String, 1, 1),
            "exists" => (Func::Exists, 1, 1),
            "abs" => (Func::Abs, 1, 1),
            "round" => (Func::Round, 1, 2),
            "floor" => (Func::Floor, 1, 1),
            "ceil" => (Func::Ceil, 1, 1),
            "coalesce" => (Func::Coalesce, 1, usize::MAX),
            "deleted" => (Func::Deleted, 0, 0),
            "uuid" => (Func::Uuid, 0, 0),
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
enum Node {
    Lit(Value),
    This,
    List(Vec<Node>),
    Field(Box<Node>, String),
    Index(Box<Node>, Box<Node>),
    Not(Box<Node>),
    Neg(Box<Node>),
    Bin(BinOp, Box<Node>, Box<Node>),
    Call(Func, Vec<Node>),
}

struct Parser {
    toks: Vec<(Tok, usize)>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> &Tok {
        &self.toks[self.pos].0
    }
    fn at(&self) -> usize {
        self.toks[self.pos].1
    }
    fn bump(&mut self) -> Tok {
        let t = self.toks[self.pos].0.clone();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        t
    }
    fn fail<T>(&self, msg: &str) -> Result<T, ExprError> {
        Err(ExprError::Parse { pos: self.at(), msg: msg.to_string() })
    }
    fn eat_op(&mut self, op: &str) -> bool {
        if matches!(self.peek(), Tok::Op(o) if *o == op) {
            self.bump();
            true
        } else {
            false
        }
    }
    fn eat_word(&mut self, w: &str) -> bool {
        if matches!(self.peek(), Tok::Ident(i) if i == w) {
            self.bump();
            true
        } else {
            false
        }
    }
    fn expect_op(&mut self, op: &str) -> Result<(), ExprError> {
        if self.eat_op(op) { Ok(()) } else { self.fail(&format!("expected `{op}`")) }
    }

    fn expr(&mut self) -> Result<Node, ExprError> {
        self.or()
    }

    fn or(&mut self) -> Result<Node, ExprError> {
        let mut l = self.and()?;
        while self.eat_op("||") || self.eat_word("or") {
            let r = self.and()?;
            l = Node::Bin(BinOp::Or, Box::new(l), Box::new(r));
        }
        Ok(l)
    }

    fn and(&mut self) -> Result<Node, ExprError> {
        let mut l = self.not()?;
        while self.eat_op("&&") || self.eat_word("and") {
            let r = self.not()?;
            l = Node::Bin(BinOp::And, Box::new(l), Box::new(r));
        }
        Ok(l)
    }

    fn not(&mut self) -> Result<Node, ExprError> {
        if self.eat_op("!") || self.eat_word("not") {
            return Ok(Node::Not(Box::new(self.not()?)));
        }
        self.cmp()
    }

    fn cmp(&mut self) -> Result<Node, ExprError> {
        let l = self.add()?;
        let op = match self.peek() {
            Tok::Op("==") => BinOp::Eq,
            Tok::Op("!=") => BinOp::Ne,
            Tok::Op("<") => BinOp::Lt,
            Tok::Op("<=") => BinOp::Le,
            Tok::Op(">") => BinOp::Gt,
            Tok::Op(">=") => BinOp::Ge,
            Tok::Ident(w) if w == "in" => BinOp::In,
            Tok::Ident(w) if w == "contains" => BinOp::Contains,
            Tok::Ident(w) if w == "startsWith" || w == "starts_with" => BinOp::StartsWith,
            Tok::Ident(w) if w == "endsWith" || w == "ends_with" => BinOp::EndsWith,
            _ => return Ok(l),
        };
        self.bump();
        let r = self.add()?;
        Ok(Node::Bin(op, Box::new(l), Box::new(r)))
    }

    fn add(&mut self) -> Result<Node, ExprError> {
        let mut l = self.mul()?;
        loop {
            let op = if self.eat_op("+") {
                BinOp::Add
            } else if self.eat_op("-") {
                BinOp::Sub
            } else {
                return Ok(l);
            };
            let r = self.mul()?;
            l = Node::Bin(op, Box::new(l), Box::new(r));
        }
    }

    fn mul(&mut self) -> Result<Node, ExprError> {
        let mut l = self.unary()?;
        loop {
            let op = if self.eat_op("*") {
                BinOp::Mul
            } else if self.eat_op("/") {
                BinOp::Div
            } else if self.eat_op("%") {
                BinOp::Rem
            } else {
                return Ok(l);
            };
            let r = self.unary()?;
            l = Node::Bin(op, Box::new(l), Box::new(r));
        }
    }

    fn unary(&mut self) -> Result<Node, ExprError> {
        if self.eat_op("-") {
            return Ok(Node::Neg(Box::new(self.unary()?)));
        }
        self.postfix()
    }

    fn postfix(&mut self) -> Result<Node, ExprError> {
        let mut n = self.primary()?;
        loop {
            if self.eat_op(".") {
                match self.bump() {
                    Tok::Ident(f) => n = Node::Field(Box::new(n), f),
                    _ => return self.fail("expected field name after `.`"),
                }
            } else if self.eat_op("[") {
                let idx = self.expr()?;
                self.expect_op("]")?;
                n = Node::Index(Box::new(n), Box::new(idx));
            } else {
                return Ok(n);
            }
        }
    }

    fn primary(&mut self) -> Result<Node, ExprError> {
        match self.bump() {
            Tok::Num(n) => Ok(Node::Lit(num(n))),
            Tok::Str(s) => Ok(Node::Lit(Value::String(s))),
            Tok::Op("(") => {
                let e = self.expr()?;
                self.expect_op(")")?;
                Ok(e)
            }
            Tok::Op("[") => {
                let mut items = Vec::new();
                if !self.eat_op("]") {
                    loop {
                        items.push(self.expr()?);
                        if self.eat_op("]") {
                            break;
                        }
                        self.expect_op(",")?;
                    }
                }
                Ok(Node::List(items))
            }
            Tok::Ident(id) => match id.as_str() {
                "true" => Ok(Node::Lit(Value::Bool(true))),
                "false" => Ok(Node::Lit(Value::Bool(false))),
                "null" => Ok(Node::Lit(Value::Null)),
                "this" => Ok(Node::This),
                name => {
                    if !self.eat_op("(") {
                        // Bare identifiers are shorthand for fields of `this`.
                        return Ok(Node::Field(Box::new(Node::This), name.to_string()));
                    }
                    let (f, min, max) =
                        Func::lookup(name).ok_or_else(|| ExprError::UnknownFunction(name.to_string()))?;
                    let mut args = Vec::new();
                    if !self.eat_op(")") {
                        loop {
                            args.push(self.expr()?);
                            if self.eat_op(")") {
                                break;
                            }
                            self.expect_op(",")?;
                        }
                    }
                    if args.len() < min || args.len() > max {
                        let expected = match (min, max) {
                            (0, 0) => "0",
                            (1, 1) => "1",
                            (2, 2) => "2",
                            (0, 1) => "0 or 1",
                            (1, 2) => "1 or 2",
                            _ => "1 or more",
                        };
                        return Err(ExprError::Arity { name: name.to_string(), expected });
                    }
                    Ok(Node::Call(f, args))
                }
            },
            _ => self.fail("expected a value"),
        }
    }
}

fn num(n: f64) -> Value {
    if n.fract() == 0.0 && n.abs() < 9.0e15 {
        Value::Number(Number::from(n as i64))
    } else {
        Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null)
    }
}

// ---------------------------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------------------------

/// A compiled filter/expression.
#[derive(Debug, Clone)]
pub struct Expr {
    root: Node,
    source: String,
}

struct Env<'a, R: RecordContext + ?Sized> {
    rec: &'a R,
    this: OnceCell<Value>,
}

impl<'a, R: RecordContext + ?Sized> Env<'a, R> {
    fn this(&self) -> &Value {
        self.this.get_or_init(|| {
            self.rec.value().and_then(|v| serde_json::from_slice(v).ok()).unwrap_or(Value::Null)
        })
    }
}

fn bytes_to_value(b: Option<&[u8]>) -> Value {
    match b {
        Some(b) => Value::String(String::from_utf8_lossy(b).into_owned()),
        None => Value::Null,
    }
}

pub fn truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(_) => true,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse().ok(),
        Value::Bool(b) => Some(*b as i32 as f64),
        _ => None,
    }
}

fn as_str(v: &Value) -> std::borrow::Cow<'_, str> {
    match v {
        Value::String(s) => s.as_str().into(),
        Value::Null => "".into(),
        other => other.to_string().into(),
    }
}

fn loose_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(_), Value::String(_)) | (Value::String(_), Value::Number(_)) => {
            match (as_f64(a), as_f64(b)) {
                (Some(x), Some(y)) => x == y,
                _ => false,
            }
        }
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

fn compare(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        _ => as_f64(a)?.partial_cmp(&as_f64(b)?),
    }
}

fn path(v: &Value, dotted: &str) -> Value {
    let mut cur = v;
    for part in dotted.split('.').filter(|p| !p.is_empty()) {
        cur = match cur {
            Value::Object(m) => match m.get(part) {
                Some(x) => x,
                None => return Value::Null,
            },
            Value::Array(a) => match part.parse::<usize>().ok().and_then(|i| a.get(i)) {
                Some(x) => x,
                None => return Value::Null,
            },
            _ => return Value::Null,
        };
    }
    cur.clone()
}

/// Sentinel returned by `deleted()`; mapping assignment removes the target field.
const DELETED_MARKER: &str = "\u{0}__bp_deleted__";

impl Expr {
    pub fn parse(src: &str) -> Result<Self, ExprError> {
        let mut p = Parser { toks: lex(src)?, pos: 0 };
        let root = p.expr()?;
        if !matches!(p.peek(), Tok::Eof) {
            return p.fail("unexpected trailing input");
        }
        Ok(Self { root, source: src.to_string() })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn eval<R: RecordContext + ?Sized>(&self, rec: &R) -> Value {
        let env = Env { rec, this: OnceCell::new() };
        eval(&self.root, &env)
    }

    /// Evaluates as a boolean predicate (filters).
    pub fn matches<R: RecordContext + ?Sized>(&self, rec: &R) -> bool {
        truthy(&self.eval(rec))
    }
}

fn eval<R: RecordContext + ?Sized>(n: &Node, env: &Env<'_, R>) -> Value {
    match n {
        Node::Lit(v) => v.clone(),
        Node::This => env.this().clone(),
        Node::List(items) => Value::Array(items.iter().map(|i| eval(i, env)).collect()),
        Node::Field(base, f) => {
            if matches!(**base, Node::This) {
                // Avoid cloning the whole document for `this.field`.
                return match env.this() {
                    Value::Object(m) => m.get(f).cloned().unwrap_or(Value::Null),
                    _ => Value::Null,
                };
            }
            match eval(base, env) {
                Value::Object(mut m) => m.remove(f).unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }
        Node::Index(base, idx) => {
            let b = eval(base, env);
            let i = eval(idx, env);
            match (&b, &i) {
                (Value::Array(a), _) => as_f64(&i).and_then(|f| a.get(f as usize)).cloned().unwrap_or(Value::Null),
                (Value::Object(m), Value::String(k)) => m.get(k).cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }
        Node::Not(x) => Value::Bool(!truthy(&eval(x, env))),
        Node::Neg(x) => as_f64(&eval(x, env)).map(|f| num(-f)).unwrap_or(Value::Null),
        Node::Bin(op, l, r) => eval_bin(*op, l, r, env),
        Node::Call(f, args) => eval_call(*f, args, env),
    }
}

fn eval_bin<R: RecordContext + ?Sized>(op: BinOp, l: &Node, r: &Node, env: &Env<'_, R>) -> Value {
    use std::cmp::Ordering::*;
    match op {
        BinOp::Or => {
            let a = eval(l, env);
            if truthy(&a) { Value::Bool(true) } else { Value::Bool(truthy(&eval(r, env))) }
        }
        BinOp::And => {
            let a = eval(l, env);
            if !truthy(&a) { Value::Bool(false) } else { Value::Bool(truthy(&eval(r, env))) }
        }
        _ => {
            let a = eval(l, env);
            let b = eval(r, env);
            match op {
                BinOp::Eq => Value::Bool(loose_eq(&a, &b)),
                BinOp::Ne => Value::Bool(!loose_eq(&a, &b)),
                BinOp::Lt => Value::Bool(compare(&a, &b) == Some(Less)),
                BinOp::Le => Value::Bool(matches!(compare(&a, &b), Some(Less | Equal))),
                BinOp::Gt => Value::Bool(compare(&a, &b) == Some(Greater)),
                BinOp::Ge => Value::Bool(matches!(compare(&a, &b), Some(Greater | Equal))),
                BinOp::In => Value::Bool(match &b {
                    Value::Array(items) => items.iter().any(|i| loose_eq(&a, i)),
                    Value::String(s) => s.contains(as_str(&a).as_ref()),
                    Value::Object(m) => m.contains_key(as_str(&a).as_ref()),
                    _ => false,
                }),
                BinOp::Contains => Value::Bool(contains(&a, &b)),
                BinOp::StartsWith => Value::Bool(as_str(&a).starts_with(as_str(&b).as_ref())),
                BinOp::EndsWith => Value::Bool(as_str(&a).ends_with(as_str(&b).as_ref())),
                BinOp::Add => match (&a, &b) {
                    (Value::String(x), y) => Value::String(format!("{x}{}", as_str(y))),
                    (x, Value::String(y)) if !matches!(x, Value::Number(_)) => {
                        Value::String(format!("{}{y}", as_str(x)))
                    }
                    _ => arith(&a, &b, |x, y| x + y),
                },
                BinOp::Sub => arith(&a, &b, |x, y| x - y),
                BinOp::Mul => arith(&a, &b, |x, y| x * y),
                BinOp::Div => arith(&a, &b, |x, y| if y == 0.0 { f64::NAN } else { x / y }),
                BinOp::Rem => arith(&a, &b, |x, y| if y == 0.0 { f64::NAN } else { x % y }),
                BinOp::Or | BinOp::And => unreachable!(),
            }
        }
    }
}

fn contains(a: &Value, b: &Value) -> bool {
    match a {
        Value::Array(items) => items.iter().any(|i| loose_eq(i, b)),
        Value::Object(m) => m.contains_key(as_str(b).as_ref()),
        other => as_str(other).contains(as_str(b).as_ref()),
    }
}

fn arith(a: &Value, b: &Value, f: impl Fn(f64, f64) -> f64) -> Value {
    match (as_f64(a), as_f64(b)) {
        (Some(x), Some(y)) => {
            let r = f(x, y);
            if r.is_nan() { Value::Null } else { num(r) }
        }
        _ => Value::Null,
    }
}

fn eval_call<R: RecordContext + ?Sized>(f: Func, args: &[Node], env: &Env<'_, R>) -> Value {
    let arg = |i: usize| eval(&args[i], env);
    match f {
        Func::Header => bytes_to_value(env.rec.header(&as_str(&arg(0)))),
        Func::Key => bytes_to_value(env.rec.key()),
        Func::Value => bytes_to_value(env.rec.value()),
        Func::Json => {
            if args.is_empty() {
                env.this().clone()
            } else {
                path(env.this(), &as_str(&arg(0)))
            }
        }
        Func::Topic => Value::String(env.rec.topic().to_string()),
        Func::Partition => Value::from(env.rec.partition()),
        Func::Offset => Value::from(env.rec.offset()),
        Func::Timestamp => Value::from(env.rec.timestamp()),
        Func::Now => Value::from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        ),
        Func::Lower => Value::String(as_str(&arg(0)).to_lowercase()),
        Func::Upper => Value::String(as_str(&arg(0)).to_uppercase()),
        Func::Len => Value::from(match arg(0) {
            Value::Array(a) => a.len(),
            Value::Object(m) => m.len(),
            Value::Null => 0,
            other => as_str(&other).chars().count(),
        }),
        Func::Contains => Value::Bool(contains(&arg(0), &arg(1))),
        Func::StartsWith => Value::Bool(as_str(&arg(0)).starts_with(as_str(&arg(1)).as_ref())),
        Func::EndsWith => Value::Bool(as_str(&arg(0)).ends_with(as_str(&arg(1)).as_ref())),
        Func::Number => as_f64(&arg(0)).map(num).unwrap_or(Value::Null),
        Func::String => Value::String(as_str(&arg(0)).into_owned()),
        Func::Exists => Value::Bool(!arg(0).is_null()),
        Func::Abs => as_f64(&arg(0)).map(|x| num(x.abs())).unwrap_or(Value::Null),
        Func::Round => {
            let digits = if args.len() > 1 { as_f64(&arg(1)).unwrap_or(0.0) } else { 0.0 };
            let m = 10f64.powi(digits as i32);
            as_f64(&arg(0)).map(|x| num((x * m).round() / m)).unwrap_or(Value::Null)
        }
        Func::Floor => as_f64(&arg(0)).map(|x| num(x.floor())).unwrap_or(Value::Null),
        Func::Ceil => as_f64(&arg(0)).map(|x| num(x.ceil())).unwrap_or(Value::Null),
        Func::Coalesce => args.iter().map(|a| eval(a, env)).find(|v| !v.is_null()).unwrap_or(Value::Null),
        Func::Deleted => Value::String(DELETED_MARKER.to_string()),
        Func::Uuid => Value::String(pseudo_uuid()),
    }
}

fn pseudo_uuid() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let c = COUNTER.fetch_add(1, Ordering::Relaxed);
    let x = (t as u64) ^ c.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let y = c.rotate_left(17) ^ (t >> 64) as u64 ^ 0xA5A5_5A5A_1234_5678;
    format!(
        "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
        (x >> 32) as u32,
        (x >> 16) as u16,
        (x as u16) & 0x0fff,
        ((y >> 48) as u16 & 0x3fff) | 0x8000,
        y & 0xffff_ffff_ffff
    )
}

// ---------------------------------------------------------------------------------------------
// Mapping (flows)
// ---------------------------------------------------------------------------------------------

/// A mapping program: one assignment per line (or `;`-separated).
///
/// ```text
/// root = this
/// root.total = this.qty * this.price
/// root.card = deleted()
/// ```
#[derive(Debug, Clone)]
pub struct Mapping {
    steps: Vec<(Vec<String>, Expr)>,
}

impl Mapping {
    pub fn parse(src: &str) -> Result<Self, ExprError> {
        let mut steps = Vec::new();
        for (lineno, raw) in src.split(['\n', ';']).enumerate() {
            let line = strip_comment(raw).trim();
            if line.is_empty() {
                continue;
            }
            let (lhs, rhs) = split_assignment(line).ok_or_else(|| ExprError::Parse {
                pos: lineno,
                msg: format!("expected `root[.field] = expression` in `{line}`"),
            })?;
            let mut parts = lhs.trim().split('.').map(str::trim);
            if parts.next() != Some("root") {
                return Err(ExprError::Parse { pos: lineno, msg: "assignment target must start with `root`".into() });
            }
            let target: Vec<String> = parts.map(str::to_string).collect();
            steps.push((target, Expr::parse(rhs.trim())?));
        }
        Ok(Self { steps })
    }

    /// Applies the mapping. Starts from the record's JSON value (or null) as `root`.
    pub fn apply<R: RecordContext + ?Sized>(&self, rec: &R) -> Value {
        let env = Env { rec, this: OnceCell::new() };
        let mut root = Value::Null;
        for (target, expr) in &self.steps {
            let v = eval(&expr.root, &env);
            let deleted = matches!(&v, Value::String(s) if s == DELETED_MARKER);
            if target.is_empty() {
                root = if deleted { Value::Null } else { v };
                continue;
            }
            if !root.is_object() {
                root = Value::Object(Map::new());
            }
            let mut cur = &mut root;
            for (i, part) in target.iter().enumerate() {
                let obj = cur.as_object_mut().unwrap();
                if i == target.len() - 1 {
                    if deleted {
                        obj.remove(part);
                    } else {
                        obj.insert(part.clone(), v.clone());
                    }
                    break;
                }
                let next = obj.entry(part.clone()).or_insert_with(|| Value::Object(Map::new()));
                if !next.is_object() {
                    *next = Value::Object(Map::new());
                }
                cur = next;
            }
        }
        root
    }
}

/// Cuts a `# comment` off a mapping line (a `#` inside a string literal is kept).
fn strip_comment(line: &str) -> &str {
    let mut in_str: Option<u8> = None;
    for (i, c) in line.bytes().enumerate() {
        match in_str {
            Some(q) if c == q => in_str = None,
            Some(_) => {}
            None if c == b'"' || c == b'\'' => in_str = Some(c),
            None if c == b'#' => return &line[..i],
            None => {}
        }
    }
    line
}

fn split_assignment(line: &str) -> Option<(&str, &str)> {
    let b = line.as_bytes();
    let mut in_str: Option<u8> = None;
    for i in 0..b.len() {
        let c = b[i];
        match in_str {
            Some(q) if c == q => in_str = None,
            Some(_) => {}
            None if c == b'"' || c == b'\'' => in_str = Some(c),
            None if c == b'=' => {
                let prev = if i > 0 { b[i - 1] } else { 0 };
                let next = b.get(i + 1).copied().unwrap_or(0);
                if next != b'=' && !matches!(prev, b'=' | b'!' | b'<' | b'>') {
                    return Some((&line[..i], &line[i + 1..]));
                }
            }
            None => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(value: &str) -> SimpleRecord {
        SimpleRecord {
            topic: "orders".into(),
            partition: 3,
            offset: 42,
            timestamp: 1000,
            key: Some(b"order-1".to_vec()),
            value: Some(value.as_bytes().to_vec()),
            headers: vec![("region".into(), b"ID-JK".to_vec()), ("type".into(), b"live".to_vec())],
        }
    }

    #[test]
    fn filters() {
        let r = rec(r#"{"amount":150000,"items":[{"sku":"A"}],"customer":{"tier":"gold"}}"#);
        let cases = [
            (r#"header("region") == "ID-JK" && header("type") != "test""#, true),
            ("this.amount > 100000", true),
            ("amount >= 150000 and customer.tier == 'gold'", true),
            ("this.items[0].sku == 'A'", true),
            (r#"json("customer.tier") in ["gold", "platinum"]"#, true),
            ("key() startsWith 'order-'", true),
            ("partition() == 3 && offset() == 42", true),
            ("not exists(this.missing)", true),
            ("this.amount * 2 == 300000", true),
            ("lower(header('region')) contains 'jk'", true),
            ("this.amount < 10", false),
            ("header('nope') == 'x'", false),
        ];
        for (src, want) in cases {
            let e = Expr::parse(src).unwrap_or_else(|e| panic!("{src}: {e}"));
            assert_eq!(e.matches(&r), want, "{src}");
        }
    }

    #[test]
    fn non_json_value_is_null() {
        let r = rec("not json");
        assert!(!Expr::parse("this.amount > 1").unwrap().matches(&r));
        assert!(Expr::parse("value() == 'not json'").unwrap().matches(&r));
    }

    #[test]
    fn parse_errors() {
        assert!(Expr::parse("this.a ==").is_err());
        assert!(matches!(Expr::parse("frobnicate(1)"), Err(ExprError::UnknownFunction(_))));
        assert!(matches!(Expr::parse("header()"), Err(ExprError::Arity { .. })));
    }

    #[test]
    fn mapping() {
        let r = rec(r#"{"qty":3,"price":2.5,"card":"4111","nested":{"a":1}}"#);
        let m = Mapping::parse(
            "# full-line comment\nroot = this  # start from the input\nroot.total = this.qty * this.price; root.card = deleted()\nroot.meta.topic = topic()\nroot.tag = \"#1\"",
        )
        .unwrap();
        let out = m.apply(&r);
        assert_eq!(out["total"], serde_json::json!(7.5));
        assert!(out.get("card").is_none());
        assert_eq!(out["meta"]["topic"], "orders");
        assert_eq!(out["nested"]["a"], 1);
        assert_eq!(out["tag"], "#1");
    }
}
