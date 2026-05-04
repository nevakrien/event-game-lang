use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Loc {
    pub range: Range<usize>,
    pub line_range: Range<usize>,
}

impl Loc {
    #[inline(always)]
    pub fn with<T>(self, value: T) -> Located<T> {
        Located { value, loc: self }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Located<T> {
    pub loc: Loc,
    pub value: T,
}

impl<T> Located<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Located<U> {
        Located {
            loc: self.loc,
            value: f(self.value),
        }
    }
}

pub type LStmt = Located<Stmt>;
pub type LExpr = Located<Expr>;

#[derive(Debug, PartialEq, Eq)]
pub enum Stmt {
    Block(Vec<LStmt>),
    If {
        cond: LExpr,
        yes: Box<LStmt>,
        no: Option<Box<LStmt>>,
    },
    Send {
        value: LExpr,
        to: LExpr,
    },
    SetColor {
        value: LExpr,
    },
    Assign {
        name: String,
        value: LExpr,
    },
    Expr(LExpr),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Expr {
    Const(Value),
    Var(String),
    Len(Box<LExpr>),
    Receive {
        from: Box<LExpr>,
    },
    Index {
        base: Box<LExpr>,
        index: Box<LExpr>,
    },
    Slice {
        base: Box<LExpr>,
        start: Option<Box<LExpr>>,
        end: Option<Box<LExpr>>,
    },
    UnaryNot(Box<LExpr>),
    Binary {
        op: BinaryOp,
        left: Box<LExpr>,
        right: Box<LExpr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Or,
    And,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Value {
    Bool(bool),
    Int(i64),
    Str(String),
    None,
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(value) => write!(f, "{value}"),
            Value::Int(value) => write!(f, "{value}"),
            Value::Str(value) => write!(f, "{value}"),
            Value::None => write!(f, "none"),
        }
    }
}

impl Value {
    fn truthy(&self) -> Result<bool, RuntimeError> {
        match self {
            Value::Bool(value) => Ok(*value),
            _ => Err(RuntimeError::Type("expected bool")),
        }
    }

    fn as_int(&self) -> Result<i64, RuntimeError> {
        match self {
            Value::Int(value) => Ok(*value),
            _ => Err(RuntimeError::Type("expected int")),
        }
    }

    fn as_str(&self) -> Result<&str, RuntimeError> {
        match self {
            Value::Str(value) => Ok(value),
            _ => Err(RuntimeError::Type("expected string")),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub from: String,
    pub to: String,
    pub value: Value,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSpec {
    pub id: String,
    pub source: String,
    pub color: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParsedNode {
    pub id: String,
    pub source: String,
    pub color: String,
    pub program: LStmt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutedLine {
    pub step: usize,
    pub node_id: String,
    pub loc: Loc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentMessageLog {
    pub step: usize,
    pub from: String,
    pub to: String,
    pub value: Value,
    pub loc: Loc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickLog {
    pub executed: Vec<ExecutedLine>,
    pub messages: Vec<SentMessageLog>,
    pub final_colors: HashMap<String, String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickResult {
    pub log: TickLog,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct TickState {
    pub work_queue: VecDeque<String>,
    pub mailboxes: HashMap<String, HashMap<String, VecDeque<Value>>>,
}

impl TickState {
    pub fn enqueue(&mut self, node_id: impl Into<String>) {
        self.work_queue.push_back(node_id.into());
    }

    pub fn send(&mut self, message: Message) {
        self.mailboxes
            .entry(message.to.clone())
            .or_default()
            .entry(message.from)
            .or_default()
            .push_back(message.value);
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseErrorKind {
    Message(String),
    UnexpectedToken,
    UnexpectedEnd,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseError {
    pub loc: Loc,
    pub kind: ParseErrorKind,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ParseErrorKind::Message(message) => write!(
                f,
                "parse error on lines {}..{}: {message}",
                self.loc.line_range.start, self.loc.line_range.end
            ),
            ParseErrorKind::UnexpectedToken => write!(
                f,
                "parse error on lines {}..{}: unexpected token",
                self.loc.line_range.start, self.loc.line_range.end
            ),
            ParseErrorKind::UnexpectedEnd => write!(
                f,
                "parse error on lines {}..{}: unexpected end of input",
                self.loc.line_range.start, self.loc.line_range.end
            ),
        }
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeError {
    Type(&'static str),
    MissingVariable(String),
    MissingNode(String),
    OutOfBounds,
    DivisionByZero,
    InvalidReceiveTarget,
}

#[derive(Debug, PartialEq, Eq)]
pub struct NodeRuntimeError {
    pub node_id: String,
    pub loc: Loc,
    pub error: RuntimeError,
}

impl fmt::Display for NodeRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "node `{}` failed on lines {}..{}: {:?}",
            self.node_id, self.loc.line_range.start, self.loc.line_range.end, self.error
        )
    }
}

impl std::error::Error for NodeRuntimeError {}

pub fn parse_program(source: &str) -> Result<LStmt, ParseError> {
    Parser::new(source).parse_program()
}

pub fn parse_node(spec: NodeSpec) -> Result<ParsedNode, ParseError> {
    let program = parse_program(&spec.source)?;
    Ok(ParsedNode {
        id: spec.id,
        source: spec.source,
        color: spec.color,
        program,
    })
}

pub fn run_tick(
    state: &mut TickState,
    registry: &HashMap<String, ParsedNode>,
) -> Result<TickResult, NodeRuntimeError> {
    let current_work_queue = std::mem::take(&mut state.work_queue);
    let current_mailboxes = std::mem::take(&mut state.mailboxes);
    let mut consumed_inboxes = HashSet::new();

    let scheduled_nodes: Vec<_> = match current_work_queue
        .iter()
        .enumerate()
        .map(|(queue_index, node_id)| {
            let node = registry.get(node_id).ok_or_else(|| NodeRuntimeError {
                node_id: node_id.clone(),
                loc: Loc {
                    range: 0..0,
                    line_range: 1..1,
                },
                error: RuntimeError::MissingNode(node_id.clone()),
            })?;
            let local_inbox = if consumed_inboxes.insert(node_id.clone()) {
                current_mailboxes.get(node_id).cloned().unwrap_or_default()
            } else {
                HashMap::new()
            };
            Ok((queue_index, node, local_inbox))
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(scheduled_nodes) => scheduled_nodes,
        Err(error) => {
            state.work_queue = current_work_queue;
            state.mailboxes = current_mailboxes;
            return Err(error);
        }
    };

    let mut node_results = match scheduled_nodes
        .into_par_iter()
        .map(|(queue_index, node, local_inbox)| run_node(queue_index, node, registry, local_inbox))
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(node_results) => node_results,
        Err(error) => {
            state.work_queue = current_work_queue;
            state.mailboxes = current_mailboxes;
            return Err(error);
        }
    };

    node_results.sort_by_key(|result| result.queue_index);

    let mut step = 0usize;
    let mut executed = Vec::new();
    let mut sent = Vec::new();
    let mut final_colors: HashMap<String, String> = registry
        .iter()
        .map(|(node_id, node)| (node_id.clone(), node.color.clone()))
        .collect();
    let mut next_state = TickState::default();

    for result in node_results {
        let node_id = result.node_id.clone();
        let mut global_steps = Vec::with_capacity(result.executed.len());
        for loc in result.executed {
            step += 1;
            global_steps.push(step);
            executed.push(ExecutedLine {
                step,
                node_id: node_id.clone(),
                loc,
            });
        }
        for pending in result.sent {
            next_state.enqueue(pending.to.clone());
            next_state.send(Message {
                from: pending.from.clone(),
                to: pending.to.clone(),
                value: pending.value.clone(),
            });
            sent.push(SentMessageLog {
                step: global_steps[pending.local_step - 1],
                from: pending.from,
                to: pending.to,
                value: pending.value,
                loc: pending.loc,
            });
        }
        final_colors.insert(result.node_id, result.color);
    }

    *state = next_state;

    Ok(TickResult {
        log: TickLog {
            executed,
            messages: sent,
            final_colors,
        },
    })
}

fn run_node(
    queue_index: usize,
    node: &ParsedNode,
    registry: &HashMap<String, ParsedNode>,
    mut local_inbox: HashMap<String, VecDeque<Value>>,
) -> Result<NodeTickResult, NodeRuntimeError> {
    let mut vars = HashMap::new();
    let mut color = node.color.clone();
    let (executed, sent) = {
        let mut ctx = EvalCtx {
            node_id: &node.id,
            vars: &mut vars,
            color: &mut color,
            inbox: &mut local_inbox,
            registry,
            executed: Vec::new(),
            sent: Vec::new(),
            local_step: 0,
        };

        eval_stmt(&node.program, &mut ctx).map_err(|error| NodeRuntimeError {
            node_id: node.id.clone(),
            loc: error.0,
            error: error.1,
        })?;

        (ctx.executed, ctx.sent)
    };

    Ok(NodeTickResult {
        queue_index,
        node_id: node.id.clone(),
        color,
        executed,
        sent,
    })
}

type LocatedRuntimeError = (Loc, RuntimeError);

struct NodeTickResult {
    queue_index: usize,
    node_id: String,
    color: String,
    executed: Vec<Loc>,
    sent: Vec<PendingSentMessageLog>,
}

struct PendingSentMessageLog {
    local_step: usize,
    from: String,
    to: String,
    value: Value,
    loc: Loc,
}

struct EvalCtx<'a> {
    node_id: &'a str,
    vars: &'a mut HashMap<String, Value>,
    color: &'a mut String,
    inbox: &'a mut HashMap<String, VecDeque<Value>>,
    registry: &'a HashMap<String, ParsedNode>,
    executed: Vec<Loc>,
    sent: Vec<PendingSentMessageLog>,
    local_step: usize,
}

fn eval_stmt(stmt: &LStmt, ctx: &mut EvalCtx<'_>) -> Result<(), LocatedRuntimeError> {
    match &stmt.value {
        Stmt::Block(items) => {
            for item in items {
                eval_stmt(item, ctx)?;
            }
        }
        Stmt::If { cond, yes, no } => {
            record_step(stmt, ctx);
            if eval_expr(cond, ctx)?
                .truthy()
                .map_err(|error| (cond.loc.clone(), error))?
            {
                eval_stmt(yes, ctx)?;
            } else if let Some(no) = no {
                eval_stmt(no, ctx)?;
            }
        }
        Stmt::Send { value, to } => {
            record_step(stmt, ctx);
            let payload = eval_expr(value, ctx)?;
            let target = eval_expr(to, ctx)?;
            let target = match target {
                Value::Str(value) => value,
                _ => {
                    return Err((
                        to.loc.clone(),
                        RuntimeError::Type("send target must be a string"),
                    ));
                }
            };
            if !ctx.registry.contains_key(&target) {
                return Err((to.loc.clone(), RuntimeError::MissingNode(target)));
            }
            let log = PendingSentMessageLog {
                local_step: ctx.local_step,
                from: ctx.node_id.to_string(),
                to: target,
                value: payload,
                loc: stmt.loc.clone(),
            };
            ctx.sent.push(log);
        }
        Stmt::SetColor { value } => {
            record_step(stmt, ctx);
            let color = eval_expr(value, ctx)?;
            let color = match color {
                Value::Str(value) => value,
                _ => {
                    return Err((
                        value.loc.clone(),
                        RuntimeError::Type("set() expects a string"),
                    ));
                }
            };
            *ctx.color = color;
        }
        Stmt::Assign { name, value } => {
            record_step(stmt, ctx);
            let result = eval_expr(value, ctx)?;
            ctx.vars.insert(name.clone(), result);
        }
        Stmt::Expr(expr) => {
            record_step(stmt, ctx);
            let _ = eval_expr(expr, ctx)?;
        }
    }
    Ok(())
}

fn record_step(stmt: &LStmt, ctx: &mut EvalCtx<'_>) {
    ctx.local_step += 1;
    ctx.executed.push(stmt.loc.clone());
}

fn eval_expr(expr: &LExpr, ctx: &mut EvalCtx<'_>) -> Result<Value, LocatedRuntimeError> {
    match &expr.value {
        Expr::Const(value) => Ok(value.clone()),
        Expr::Var(name) => ctx.vars.get(name).cloned().ok_or_else(|| {
            (
                expr.loc.clone(),
                RuntimeError::MissingVariable(name.clone()),
            )
        }),
        Expr::Len(inner) => {
            let value = eval_expr(inner, ctx)?;
            match value {
                Value::Str(value) => Ok(Value::Int(value.chars().count() as i64)),
                _ => Err((
                    inner.loc.clone(),
                    RuntimeError::Type("len() expects a string"),
                )),
            }
        }
        Expr::Receive { from } => {
            let from_value = eval_expr(from, ctx)?;
            let from = match from_value {
                Value::Str(value) => value,
                _ => return Err((from.loc.clone(), RuntimeError::InvalidReceiveTarget)),
            };
            Ok(ctx
                .inbox
                .get_mut(&from)
                .and_then(VecDeque::pop_front)
                .unwrap_or(Value::None))
        }
        Expr::Index { base, index } => {
            let base_value = eval_expr(base, ctx)?;
            let index_value = eval_expr(index, ctx)?;
            let text = base_value
                .as_str()
                .map_err(|error| (base.loc.clone(), error))?;
            let index_value = index_value
                .as_int()
                .map_err(|error| (index.loc.clone(), error))?;
            let index: usize = index_value
                .try_into()
                .map_err(|_| (index.loc.clone(), RuntimeError::OutOfBounds))?;
            let ch = text
                .chars()
                .nth(index)
                .ok_or_else(|| (expr.loc.clone(), RuntimeError::OutOfBounds))?;
            Ok(Value::Str(ch.to_string()))
        }
        Expr::Slice { base, start, end } => {
            let base_value = eval_expr(base, ctx)?;
            let text = base_value
                .as_str()
                .map_err(|error| (base.loc.clone(), error))?;
            let chars: Vec<char> = text.chars().collect();
            let start = match start {
                Some(start) => eval_expr(start, ctx)?
                    .as_int()
                    .map_err(|error| (start.loc.clone(), error))?,
                None => 0,
            };
            let end = match end {
                Some(end) => eval_expr(end, ctx)?
                    .as_int()
                    .map_err(|error| (end.loc.clone(), error))?,
                None => chars.len() as i64,
            };
            let start: usize = start
                .try_into()
                .map_err(|_| (expr.loc.clone(), RuntimeError::OutOfBounds))?;
            let end: usize = end
                .try_into()
                .map_err(|_| (expr.loc.clone(), RuntimeError::OutOfBounds))?;
            if start > end || end > chars.len() {
                return Err((expr.loc.clone(), RuntimeError::OutOfBounds));
            }
            Ok(Value::Str(chars[start..end].iter().collect()))
        }
        Expr::UnaryNot(inner) => {
            let value = eval_expr(inner, ctx)?;
            Ok(Value::Bool(
                !value.truthy().map_err(|error| (inner.loc.clone(), error))?,
            ))
        }
        Expr::Binary { op, left, right } => eval_binary(*op, left, right, ctx),
    }
}

fn eval_binary(
    op: BinaryOp,
    left: &LExpr,
    right: &LExpr,
    ctx: &mut EvalCtx<'_>,
) -> Result<Value, LocatedRuntimeError> {
    match op {
        BinaryOp::Or => Ok(Value::Bool(
            eval_expr(left, ctx)?
                .truthy()
                .map_err(|error| (left.loc.clone(), error))?
                || eval_expr(right, ctx)?
                    .truthy()
                    .map_err(|error| (right.loc.clone(), error))?,
        )),
        BinaryOp::And => Ok(Value::Bool(
            eval_expr(left, ctx)?
                .truthy()
                .map_err(|error| (left.loc.clone(), error))?
                && eval_expr(right, ctx)?
                    .truthy()
                    .map_err(|error| (right.loc.clone(), error))?,
        )),
        BinaryOp::Equal => Ok(Value::Bool(eval_expr(left, ctx)? == eval_expr(right, ctx)?)),
        BinaryOp::NotEqual => Ok(Value::Bool(eval_expr(left, ctx)? != eval_expr(right, ctx)?)),
        BinaryOp::Less => Ok(Value::Bool(
            eval_expr(left, ctx)?
                .as_int()
                .map_err(|error| (left.loc.clone(), error))?
                < eval_expr(right, ctx)?
                    .as_int()
                    .map_err(|error| (right.loc.clone(), error))?,
        )),
        BinaryOp::LessEqual => Ok(Value::Bool(
            eval_expr(left, ctx)?
                .as_int()
                .map_err(|error| (left.loc.clone(), error))?
                <= eval_expr(right, ctx)?
                    .as_int()
                    .map_err(|error| (right.loc.clone(), error))?,
        )),
        BinaryOp::Greater => Ok(Value::Bool(
            eval_expr(left, ctx)?
                .as_int()
                .map_err(|error| (left.loc.clone(), error))?
                > eval_expr(right, ctx)?
                    .as_int()
                    .map_err(|error| (right.loc.clone(), error))?,
        )),
        BinaryOp::GreaterEqual => Ok(Value::Bool(
            eval_expr(left, ctx)?
                .as_int()
                .map_err(|error| (left.loc.clone(), error))?
                >= eval_expr(right, ctx)?
                    .as_int()
                    .map_err(|error| (right.loc.clone(), error))?,
        )),
        BinaryOp::Add => {
            let left_value = eval_expr(left, ctx)?;
            let right_value = eval_expr(right, ctx)?;
            match (left_value, right_value) {
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
                (Value::Str(a), Value::Str(b)) => Ok(Value::Str(a + &b)),
                _ => Err((
                    left.loc.clone(),
                    RuntimeError::Type("+ expects matching ints or strings"),
                )),
            }
        }
        BinaryOp::Sub => Ok(Value::Int(
            eval_expr(left, ctx)?
                .as_int()
                .map_err(|error| (left.loc.clone(), error))?
                - eval_expr(right, ctx)?
                    .as_int()
                    .map_err(|error| (right.loc.clone(), error))?,
        )),
        BinaryOp::Mul => Ok(Value::Int(
            eval_expr(left, ctx)?
                .as_int()
                .map_err(|error| (left.loc.clone(), error))?
                * eval_expr(right, ctx)?
                    .as_int()
                    .map_err(|error| (right.loc.clone(), error))?,
        )),
        BinaryOp::Div => {
            let right_value = eval_expr(right, ctx)?
                .as_int()
                .map_err(|error| (right.loc.clone(), error))?;
            if right_value == 0 {
                return Err((right.loc.clone(), RuntimeError::DivisionByZero));
            }
            Ok(Value::Int(
                eval_expr(left, ctx)?
                    .as_int()
                    .map_err(|error| (left.loc.clone(), error))?
                    / right_value,
            ))
        }
        BinaryOp::Mod => {
            let right_value = eval_expr(right, ctx)?
                .as_int()
                .map_err(|error| (right.loc.clone(), error))?;
            if right_value == 0 {
                return Err((right.loc.clone(), RuntimeError::DivisionByZero));
            }
            Ok(Value::Int(
                eval_expr(left, ctx)?
                    .as_int()
                    .map_err(|error| (left.loc.clone(), error))?
                    % right_value,
            ))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TokenKind {
    Ident(String),
    String(String),
    Int(i64),
    Symbol(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Token {
    kind: TokenKind,
    loc: Loc,
}

struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    cursor: usize,
}

impl<'a> Parser<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            tokens: tokenize(source),
            cursor: 0,
        }
    }

    fn parse_program(mut self) -> Result<LStmt, ParseError> {
        let start = self
            .tokens
            .first()
            .map(|token| token.loc.clone())
            .unwrap_or_else(|| loc_for(self.source, 0, 0));
        let mut stmts = Vec::new();
        while !self.is_done() {
            stmts.push(self.parse_stmt()?);
        }
        let end = stmts
            .last()
            .map(|stmt| stmt.loc.clone())
            .unwrap_or(start.clone());
        Ok(Loc {
            range: start.range.start..end.range.end,
            line_range: start.line_range.start..end.line_range.end,
        }
        .with(Stmt::Block(stmts)))
    }

    fn parse_stmt(&mut self) -> Result<LStmt, ParseError> {
        if self.match_ident("if") {
            return self.parse_if();
        }
        if self.match_ident("send") {
            return self.parse_send();
        }
        if self.peek_ident("set") {
            return self.parse_set();
        }
        if let Some(name) = self.peek_assign_name() {
            let name_token = self.advance().unwrap().clone();
            self.expect_symbol("=")?;
            let value = self.parse_expr()?;
            let loc = merge_loc(&name_token.loc, &value.loc);
            return Ok(loc.with(Stmt::Assign { name, value }));
        }
        let expr = self.parse_expr()?;
        Ok(expr.loc.clone().with(Stmt::Expr(expr)))
    }

    fn parse_if(&mut self) -> Result<LStmt, ParseError> {
        let start = self.previous().unwrap().loc.clone();
        let cond = self.parse_expr()?;
        let yes = self.parse_block()?;
        let no = if self.match_ident("else") {
            Some(Box::new(self.parse_block()?))
        } else {
            None
        };
        let end_loc = no
            .as_ref()
            .map(|stmt| stmt.loc.clone())
            .unwrap_or_else(|| yes.loc.clone());
        Ok(merge_loc(&start, &end_loc).with(Stmt::If {
            cond,
            yes: Box::new(yes),
            no,
        }))
    }

    fn parse_send(&mut self) -> Result<LStmt, ParseError> {
        let start = self.previous().unwrap().loc.clone();
        let (value, to, end) = if self.match_symbol("(") {
            let value = self.parse_expr()?;
            self.expect_symbol(",")?;
            let to = self.parse_expr()?;
            let end = self.expect_symbol(")")?.loc;
            (value, to, end)
        } else {
            let value = self.parse_expr()?;
            self.expect_ident("to")?;
            let to = self.parse_expr()?;
            let end = to.loc.clone();
            (value, to, end)
        };
        Ok(merge_loc(&start, &end).with(Stmt::Send { value, to }))
    }

    fn parse_set(&mut self) -> Result<LStmt, ParseError> {
        let start = self.advance().unwrap().loc.clone();
        self.expect_symbol("(")?;
        let value = self.parse_expr()?;
        let end = self.expect_symbol(")")?.loc;
        Ok(merge_loc(&start, &end).with(Stmt::SetColor { value }))
    }

    fn parse_block(&mut self) -> Result<LStmt, ParseError> {
        let start = self.expect_symbol("{")?.loc;
        let mut stmts = Vec::new();
        while !self.peek_symbol("}") {
            if self.is_done() {
                return Err(ParseError {
                    loc: start.clone(),
                    kind: ParseErrorKind::UnexpectedEnd,
                });
            }
            stmts.push(self.parse_stmt()?);
        }
        let end = self.expect_symbol("}")?.loc;
        Ok(merge_loc(&start, &end).with(Stmt::Block(stmts)))
    }

    fn parse_expr(&mut self) -> Result<LExpr, ParseError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<LExpr, ParseError> {
        self.parse_left_assoc(Self::parse_and, &["||"])
    }

    fn parse_and(&mut self) -> Result<LExpr, ParseError> {
        self.parse_left_assoc(Self::parse_compare, &["&&"])
    }

    fn parse_compare(&mut self) -> Result<LExpr, ParseError> {
        self.parse_left_assoc(Self::parse_add, &["==", "!=", "<", "<=", ">", ">="])
    }

    fn parse_add(&mut self) -> Result<LExpr, ParseError> {
        self.parse_left_assoc(Self::parse_mul, &["+", "-"])
    }

    fn parse_mul(&mut self) -> Result<LExpr, ParseError> {
        self.parse_left_assoc(Self::parse_unary, &["*", "/", "%"])
    }

    fn parse_left_assoc(
        &mut self,
        mut parse_inner: impl FnMut(&mut Self) -> Result<LExpr, ParseError>,
        ops: &[&str],
    ) -> Result<LExpr, ParseError> {
        let mut expr = parse_inner(self)?;
        while let Some(op) = self.match_any_symbol(ops) {
            let right = parse_inner(self)?;
            let loc = merge_loc(&expr.loc, &right.loc);
            expr = loc.with(Expr::Binary {
                op: map_binary_op(op),
                left: Box::new(expr),
                right: Box::new(right),
            });
        }
        Ok(expr)
    }

    fn parse_unary(&mut self) -> Result<LExpr, ParseError> {
        if self.match_symbol("!") {
            let start = self.previous().unwrap().loc.clone();
            let inner = self.parse_unary()?;
            return Ok(merge_loc(&start, &inner.loc).with(Expr::UnaryNot(Box::new(inner))));
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<LExpr, ParseError> {
        let mut expr = self.parse_primary()?;
        while self.match_symbol("[") {
            let open = self.previous().unwrap().loc.clone();
            if self.match_symbol(":") {
                let end = if self.peek_symbol("]") {
                    None
                } else {
                    Some(Box::new(self.parse_expr()?))
                };
                let close = self.expect_symbol("]")?.loc;
                expr = merge_loc(&expr.loc, &close).with(Expr::Slice {
                    base: Box::new(expr),
                    start: None,
                    end,
                });
            } else {
                let first = self.parse_expr()?;
                if self.match_symbol(":") {
                    let end = if self.peek_symbol("]") {
                        None
                    } else {
                        Some(Box::new(self.parse_expr()?))
                    };
                    let close = self.expect_symbol("]")?.loc;
                    expr = merge_loc(&expr.loc, &close).with(Expr::Slice {
                        base: Box::new(expr),
                        start: Some(Box::new(first)),
                        end,
                    });
                } else {
                    let close = self.expect_symbol("]")?.loc;
                    let _ = open;
                    expr = merge_loc(&expr.loc, &close).with(Expr::Index {
                        base: Box::new(expr),
                        index: Box::new(first),
                    });
                }
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<LExpr, ParseError> {
        let token = self.advance().cloned().ok_or(ParseError {
            loc: loc_for(self.source, self.source.len(), self.source.len()),
            kind: ParseErrorKind::UnexpectedEnd,
        })?;
        match token.kind {
            TokenKind::Int(value) => Ok(token.loc.with(Expr::Const(Value::Int(value)))),
            TokenKind::String(value) => Ok(token.loc.with(Expr::Const(Value::Str(value)))),
            TokenKind::Ident(name) => match name.as_str() {
                "none" => Ok(token.loc.with(Expr::Const(Value::None))),
                "true" => Ok(token.loc.with(Expr::Const(Value::Bool(true)))),
                "false" => Ok(token.loc.with(Expr::Const(Value::Bool(false)))),
                "len" => {
                    self.expect_symbol("(")?;
                    let inner = self.parse_expr()?;
                    let end = self.expect_symbol(")")?.loc;
                    Ok(merge_loc(&token.loc, &end).with(Expr::Len(Box::new(inner))))
                }
                "receive" | "recive" => {
                    let (from, end) = if self.match_symbol("(") {
                        let from = self.parse_expr()?;
                        let end = self.expect_symbol(")")?.loc;
                        (from, end)
                    } else {
                        self.expect_ident("from")?;
                        let from = self.parse_expr()?;
                        let end = from.loc.clone();
                        (from, end)
                    };
                    Ok(merge_loc(&token.loc, &end).with(Expr::Receive {
                        from: Box::new(from),
                    }))
                }
                _ => Ok(token.loc.with(Expr::Var(name))),
            },
            TokenKind::Symbol("(") => {
                let expr = self.parse_expr()?;
                self.expect_symbol(")")?;
                Ok(expr)
            }
            _ => Err(ParseError {
                loc: token.loc,
                kind: ParseErrorKind::UnexpectedToken,
            }),
        }
    }

    fn is_done(&self) -> bool {
        self.cursor >= self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    fn previous(&self) -> Option<&Token> {
        self.tokens.get(self.cursor.saturating_sub(1))
    }

    fn advance(&mut self) -> Option<&Token> {
        let token = self.tokens.get(self.cursor);
        if token.is_some() {
            self.cursor += 1;
        }
        token
    }

    fn peek_symbol(&self, symbol: &str) -> bool {
        matches!(self.peek().map(|t| &t.kind), Some(TokenKind::Symbol(current)) if *current == symbol)
    }

    fn match_symbol(&mut self, symbol: &str) -> bool {
        if self.peek_symbol(symbol) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn match_any_symbol(&mut self, symbols: &[&str]) -> Option<&'static str> {
        for symbol in symbols {
            if self.peek_symbol(symbol) {
                self.cursor += 1;
                return Some(Box::leak(symbol.to_string().into_boxed_str()));
            }
        }
        None
    }

    fn expect_symbol(&mut self, symbol: &str) -> Result<Token, ParseError> {
        let token = self.advance().cloned().ok_or(ParseError {
            loc: loc_for(self.source, self.source.len(), self.source.len()),
            kind: ParseErrorKind::UnexpectedEnd,
        })?;
        match token.kind {
            TokenKind::Symbol(current) if current == symbol => Ok(token),
            _ => Err(ParseError {
                loc: token.loc,
                kind: ParseErrorKind::Message(format!("expected `{symbol}`")),
            }),
        }
    }

    fn peek_ident(&self, ident: &str) -> bool {
        matches!(self.peek().map(|t| &t.kind), Some(TokenKind::Ident(current)) if current == ident)
    }

    fn match_ident(&mut self, ident: &str) -> bool {
        if self.peek_ident(ident) {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    fn expect_ident(&mut self, ident: &str) -> Result<(), ParseError> {
        let token = self.advance().cloned().ok_or(ParseError {
            loc: loc_for(self.source, self.source.len(), self.source.len()),
            kind: ParseErrorKind::UnexpectedEnd,
        })?;
        match token.kind {
            TokenKind::Ident(current) if current == ident => Ok(()),
            _ => Err(ParseError {
                loc: token.loc,
                kind: ParseErrorKind::Message(format!("expected `{ident}`")),
            }),
        }
    }

    fn peek_assign_name(&self) -> Option<String> {
        let first = self.tokens.get(self.cursor)?;
        let second = self.tokens.get(self.cursor + 1)?;
        match (&first.kind, &second.kind) {
            (TokenKind::Ident(name), TokenKind::Symbol("=")) => Some(name.clone()),
            _ => None,
        }
    }
}

fn map_binary_op(symbol: &str) -> BinaryOp {
    match symbol {
        "||" => BinaryOp::Or,
        "&&" => BinaryOp::And,
        "==" => BinaryOp::Equal,
        "!=" => BinaryOp::NotEqual,
        "<" => BinaryOp::Less,
        "<=" => BinaryOp::LessEqual,
        ">" => BinaryOp::Greater,
        ">=" => BinaryOp::GreaterEqual,
        "+" => BinaryOp::Add,
        "-" => BinaryOp::Sub,
        "*" => BinaryOp::Mul,
        "/" => BinaryOp::Div,
        "%" => BinaryOp::Mod,
        _ => unreachable!(),
    }
}

fn tokenize(source: &str) -> Vec<Token> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b' ' | b'\t' | b'\r' | b'\n' => {
                index += 1;
            }
            b'/' if bytes.get(index + 1) == Some(&b'/') => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            b'0'..=b'9' => {
                let start = index;
                while index < bytes.len() && bytes[index].is_ascii_digit() {
                    index += 1;
                }
                let value = source[start..index].parse().expect("integer token");
                tokens.push(Token {
                    kind: TokenKind::Int(value),
                    loc: loc_for(source, start, index),
                });
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                let start = index;
                while index < bytes.len()
                    && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
                {
                    index += 1;
                }
                tokens.push(Token {
                    kind: TokenKind::Ident(source[start..index].to_string()),
                    loc: loc_for(source, start, index),
                });
            }
            b'"' => {
                let start = index;
                index += 1;
                let mut value = String::new();
                while index < bytes.len() {
                    match bytes[index] {
                        b'\\' => {
                            index += 1;
                            if index >= bytes.len() {
                                break;
                            }
                            let escaped = match bytes[index] {
                                b'n' => '\n',
                                b'r' => '\r',
                                b't' => '\t',
                                b'"' => '"',
                                b'\\' => '\\',
                                other => other as char,
                            };
                            value.push(escaped);
                            index += 1;
                        }
                        b'"' => {
                            index += 1;
                            break;
                        }
                        other => {
                            value.push(other as char);
                            index += 1;
                        }
                    }
                }
                tokens.push(Token {
                    kind: TokenKind::String(value),
                    loc: loc_for(source, start, index),
                });
            }
            _ => {
                let start = index;
                let symbol = if let Some(two) = source.get(index..index + 2) {
                    match two {
                        "==" | "!=" | "<=" | ">=" | "&&" | "||" => {
                            index += 2;
                            Some(two)
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                if let Some(symbol) = symbol {
                    tokens.push(Token {
                        kind: TokenKind::Symbol(match symbol {
                            "==" => "==",
                            "!=" => "!=",
                            "<=" => "<=",
                            ">=" => ">=",
                            "&&" => "&&",
                            "||" => "||",
                            _ => unreachable!(),
                        }),
                        loc: loc_for(source, start, index),
                    });
                    continue;
                }

                let single = match bytes[index] as char {
                    '{' => Some("{"),
                    '}' => Some("}"),
                    '(' => Some("("),
                    ')' => Some(")"),
                    '[' => Some("["),
                    ']' => Some("]"),
                    ':' => Some(":"),
                    ',' => Some(","),
                    '+' => Some("+"),
                    '-' => Some("-"),
                    '*' => Some("*"),
                    '/' => Some("/"),
                    '%' => Some("%"),
                    '<' => Some("<"),
                    '>' => Some(">"),
                    '=' => Some("="),
                    '!' => Some("!"),
                    _ => None,
                };
                index += 1;
                if let Some(single) = single {
                    tokens.push(Token {
                        kind: TokenKind::Symbol(single),
                        loc: loc_for(source, start, index),
                    });
                }
            }
        }
    }

    tokens
}

fn merge_loc(start: &Loc, end: &Loc) -> Loc {
    Loc {
        range: start.range.start..end.range.end,
        line_range: start.line_range.start..end.line_range.end,
    }
}

fn loc_for(source: &str, start: usize, end: usize) -> Loc {
    Loc {
        range: start..end,
        line_range: line_for(source, start)
            ..line_for(source, end.saturating_sub(1)).saturating_add(1),
    }
}

fn line_for(source: &str, offset: usize) -> usize {
    if source.is_empty() {
        return 1;
    }
    let capped = offset.min(source.len().saturating_sub(1));
    source[..=capped]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registry(nodes: Vec<ParsedNode>) -> HashMap<String, ParsedNode> {
        nodes
            .into_iter()
            .map(|node| (node.id.clone(), node))
            .collect()
    }

    fn mailbox_values(state: &TickState, to: &str, from: &str) -> Vec<Value> {
        state
            .mailboxes
            .get(to)
            .and_then(|mailbox| mailbox.get(from))
            .map(|queue| queue.iter().cloned().collect())
            .unwrap_or_default()
    }

    #[test]
    fn parses_readme_style_program() {
        let source = r#"
if true {
    send "hi" to "right"
} else {
    b = recive from "left"
}

if(len("abcd")>2){
    set("blue")
}
"#;

        let parsed = parse_program(source).expect("program parses");
        match parsed.value {
            Stmt::Block(stmts) => assert_eq!(stmts.len(), 2),
            other => panic!("unexpected root: {other:?}"),
        }
    }

    #[test]
    fn runs_one_tick_and_logs_execution() {
        let left = parse_node(NodeSpec {
            id: "left".into(),
            color: "gray".into(),
            source: r#"
msg = receive from "right"
if msg != none {
    set(msg[2:])
}
"#
            .into(),
        })
        .expect("left parses");
        let right = parse_node(NodeSpec {
            id: "right".into(),
            color: "black".into(),
            source: "send \"xxgreen\" to \"left\"".into(),
        })
        .expect("right parses");
        let registry = registry(vec![left, right]);
        let mut state = TickState::default();
        state.enqueue("left");
        state.enqueue("right");
        state.send(Message {
            from: "right".into(),
            to: "left".into(),
            value: Value::Str("xxgreen".into()),
        });

        let tick = run_tick(&mut state, &registry).expect("tick runs");

        assert_eq!(tick.log.executed.len(), 4);
        assert_eq!(tick.log.messages.len(), 1);
        assert_eq!(
            tick.log.final_colors.get("left"),
            Some(&"green".to_string())
        );
        assert_eq!(
            tick.log.final_colors.get("right"),
            Some(&"black".to_string())
        );
        assert_eq!(tick.log.executed[0].loc.line_range, 2..3);
        assert_eq!(tick.log.messages[0].to, "left");
        assert_eq!(
            state.work_queue.iter().collect::<Vec<_>>(),
            vec![&"left".to_string()]
        );
        assert_eq!(
            mailbox_values(&state, "left", "right"),
            vec![Value::Str("xxgreen".into())]
        );
    }

    #[test]
    fn send_is_visible_next_tick_not_same_tick() {
        let center = parse_node(NodeSpec {
            id: "center".into(),
            color: "gray".into(),
            source: r#"
msg = receive from "left"
if msg != none {
    set(msg)
}
send "pong" to "left"
"#
            .into(),
        })
        .expect("node parses");
        let left = parse_node(NodeSpec {
            id: "left".into(),
            color: "gray".into(),
            source: "".into(),
        })
        .expect("left parses");
        let registry = registry(vec![center, left]);
        let mut state = TickState::default();
        state.enqueue("center");
        state.enqueue("left");

        let first_tick = run_tick(&mut state, &registry).expect("first tick runs");
        assert_eq!(
            first_tick.log.final_colors.get("center"),
            Some(&"gray".to_string())
        );
        assert_eq!(
            state.work_queue.iter().collect::<Vec<_>>(),
            vec![&"left".to_string()]
        );
        assert_eq!(
            mailbox_values(&state, "left", "center"),
            vec![Value::Str("pong".into())]
        );

        state.send(Message {
            from: "left".into(),
            to: "center".into(),
            value: Value::Str("blue".into()),
        });
        state.enqueue("center");

        let second_tick = run_tick(&mut state, &registry).expect("second tick runs");
        assert_eq!(
            second_tick.log.final_colors.get("center"),
            Some(&"blue".to_string())
        );
    }

    #[test]
    fn repeated_scheduling_consumes_inbox_only_once() {
        let left = parse_node(NodeSpec {
            id: "left".into(),
            color: "gray".into(),
            source: r#"
msg = receive from "right"
if msg != none {
    set(msg)
}
"#
            .into(),
        })
        .expect("left parses");
        let right = parse_node(NodeSpec {
            id: "right".into(),
            color: "black".into(),
            source: "".into(),
        })
        .expect("right parses");
        let registry = registry(vec![left, right]);
        let mut state = TickState::default();
        state.enqueue("left");
        state.enqueue("left");
        state.send(Message {
            from: "right".into(),
            to: "left".into(),
            value: Value::Str("blue".into()),
        });

        let tick = run_tick(&mut state, &registry).expect("tick runs");

        assert_eq!(tick.log.messages.len(), 0);
        assert_eq!(tick.log.final_colors.get("left"), Some(&"gray".to_string()));
        assert_eq!(mailbox_values(&state, "left", "right"), Vec::<Value>::new());
    }
}
