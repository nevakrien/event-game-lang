pub enum Op {
	If{
		cond:Expr,
		yes:Box<Op>,
		no:Option<Box<Op>>,
	},
	Send{
		from:Expr,
		value:Expr,
	},
	Assign{
		name:String,
		value:Expr
	},
	Exp(Expr),
}

#[derive(Clone)]
pub enum Value {
	Bool(bool),
	Str(Box<str>),
	Int(i64),
}

#[derive(Clone)]
pub enum Expr {
	Const(Value),
	Recive{to:Value},
	Var{name:String},

	And(Box<[Expr;2]>),
	Or(Box<[Expr;2]>),
	Not(Box<Expr>),

	Equal(Box<[Expr;2]>),
	NotEqual(Box<[Expr;2]>),
	LessEqual(Box<[Expr;2]>),
	Less(Box<[Expr;2]>),
	Greater(Box<[Expr;2]>),
	GreaterEqual(Box<[Expr;2]>),

	Index{
		base:Box<Expr>,
		spot:Box<Expr>,
	},

	RangeIndex{
		base:Box<Expr>,
		start:Option<Box<Expr>>,
		end:Option<Box<Expr>>,
	},

	Add(Box<[Expr;2]>),
	Sub(Box<[Expr;2]>),
	Mul(Box<[Expr;2]>),
	Div(Box<[Expr;2]>),
}