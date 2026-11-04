// Builds the six-slide hackathon deck for the LDK Node liquidity agent.
//
//   NODE_PATH=<dir with pptxgenjs> APPLY_THEME=<pptx skill>/scripts/apply_theme.js \
//     node build-slides.js [out.pptx]
//
// Written with Claude Code.

const path = require("path");
const pptxgen = require("pptxgenjs");
const { applyTheme } = require(process.env.APPLY_THEME ||
	path.join(__dirname, "apply_theme.js"));

const OUT = process.argv[2] || path.join(__dirname, "ldk-liquidity-agent.pptx");

const THEME = {
	name: "LDK Liquidity Agent",
	headFontFace: "Calibri",
	bodyFontFace: "Calibri",
	colors: {
		dk1: "0B1526", // near-black navy: body text
		lt1: "FFFFFF",
		dk2: "16213E", // navy: dark backgrounds, headings
		lt2: "EEF2F8", // ice: cards on light slides
		accent1: "F7931A", // bitcoin orange: our node, the agent, emphasis
		accent2: "2B6CB0", // steel blue: candidate peers, the GDK
		accent3: "E53E3E", // red: what went wrong
		accent4: "38A169", // green: destinations, verified
		accent5: "FFD9A8", // pale orange: highlights on dark
		accent6: "8A94A6", // gray: the rest of the graph, secondary text
		hlink: "2B6CB0",
		folHlink: "7B2CFF",
	},
};

const pres = new pptxgen();
pres.layout = "LAYOUT_16x9"; // 10 x 5.625 in
pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
pres.author = "LDK Node hackathon";
pres.title = "Where should the money go?";
const C = pres.SchemeColor;

// ---------------------------------------------------------------- layouts

const FOOTER = "LDK Node + GDK liquidity agent";

pres.defineSlideMaster({
	title: "DARK",
	background: { color: C.text2 },
	objects: [
		{ placeholder: { options: { name: "title", type: "title", x: 0.6, y: 1.5, w: 8.8, h: 1.2,
			fontSize: 40, bold: true, color: C.background1, margin: 0, valign: "bottom", align: "left" } } },
		{ placeholder: { options: { name: "body", type: "body", x: 0.6, y: 2.75, w: 8.8, h: 0.9,
			fontSize: 20, color: C.accent5, margin: 0, valign: "top", align: "left" } } },
	],
});

pres.defineSlideMaster({
	title: "CONTENT",
	background: { color: C.background1 },
	objects: [
		{ placeholder: { options: { name: "title", type: "title", x: 0.5, y: 0.3, w: 9.0, h: 0.7,
			fontSize: 28, bold: true, color: C.text2, margin: 0, valign: "middle", align: "left" } } },
		{ text: { text: FOOTER, options: { x: 0.5, y: 5.25, w: 6, h: 0.3, fontSize: 9,
			color: C.accent6, margin: 0 } } },
	],
	slideNumber: { x: 9.2, y: 5.25, w: 0.4, h: 0.3, fontSize: 9, color: C.accent6, align: "right" },
});

pres.defineSlideMaster({
	title: "DARK_CONTENT",
	background: { color: C.text2 },
	objects: [
		{ placeholder: { options: { name: "title", type: "title", x: 0.5, y: 0.3, w: 9.0, h: 0.7,
			fontSize: 28, bold: true, color: C.background1, margin: 0, valign: "middle", align: "left" } } },
		{ text: { text: FOOTER, options: { x: 0.5, y: 5.25, w: 6, h: 0.3, fontSize: 9,
			color: C.accent6, margin: 0 } } },
	],
	slideNumber: { x: 9.2, y: 5.25, w: 0.4, h: 0.3, fontSize: 9, color: C.accent6, align: "right" },
});

// ---------------------------------------------------------------- helpers

function text(slide, str, o) {
	slide.addText(str, { isTextBox: true, margin: 0, fontSize: 11, color: C.text1, ...o });
}

// Rounded card with optional text.
function card(slide, o, str, textOpts) {
	const base = { shape: pres.ShapeType.roundRect, rectRadius: 0.08, fill: { color: C.background2 },
		line: { color: C.background2, width: 0 } };
	slide.addText(str || "", { ...base, margin: 0.08, fontSize: 10, color: C.text1, valign: "top",
		align: "left", ...o, ...(textOpts || {}) });
}

// Circle with a short label inside.
function circle(slide, cx, cy, d, label, o) {
	slide.addText(label, { shape: pres.ShapeType.ellipse, x: cx - d / 2, y: cy - d / 2, w: d, h: d,
		fill: { color: C.accent6 }, line: { color: C.accent6, width: 0 }, align: "center",
		valign: "middle", fontSize: 12, bold: true, color: C.background1, margin: 0, ...o });
}

// Straight connector from (x1,y1) to (x2,y2), arrow at the end unless told otherwise.
function arrow(slide, x1, y1, x2, y2, o) {
	const opts = { color: C.accent6, width: 1.5, endArrowType: "triangle", ...(o || {}) };
	const x = Math.min(x1, x2), y = Math.min(y1, y2);
	const w = Math.abs(x2 - x1), h = Math.abs(y2 - y1);
	const flipH = x2 < x1, flipV = y2 < y1;
	slide.addShape(pres.ShapeType.line, { x, y, w, h, flipH, flipV, line: opts });
}

// A channel drawn as a bar: the filled part is the balance on our side.
function channelBar(slide, x, y, w, h, localPct, o) {
	const colorLocal = (o && o.localColor) || C.accent1;
	const lw = Math.max(w * localPct / 100, 0.02);
	slide.addShape(pres.ShapeType.rect, { x, y, w, h, fill: { color: C.accent6 }, line: { color: C.accent6, width: 0 } });
	slide.addShape(pres.ShapeType.rect, { x, y, w: lw, h, fill: { color: colorLocal }, line: { color: colorLocal, width: 0 } });
}

// Icon circle + heading + body, the block used on the right-hand columns.
function block(slide, x, y, w, ico, color, head, body, o) {
	const opts = { headSize: 12.5, bodySize: 10, bodyH: 0.9, ...(o || {}) };
	circle(slide, x + 0.25, y + 0.25, 0.5, ico, { fill: { color }, line: { color, width: 0 }, fontSize: 9 });
	text(slide, head, { x: x + 0.65, y: y - 0.02, w: w - 0.65, h: 0.3, fontSize: opts.headSize, bold: true, color: opts.headColor || C.text2 });
	text(slide, body, { x: x + 0.65, y: y + 0.3, w: w - 0.65, h: opts.bodyH, fontSize: opts.bodySize, color: opts.bodyColor || C.text1, valign: "top" });
}

// A plain dot, used for the anonymous nodes of the gossip graph.
function dot(slide, cx, cy, d, color) {
	slide.addShape(pres.ShapeType.ellipse, { x: cx - d / 2, y: cy - d / 2, w: d, h: d,
		fill: { color }, line: { color, width: 0 } });
}

// ---------------------------------------------------------------- slide 1: title

pres.addSection({ title: "Title" });

{
	const s = pres.addSlide({ masterName: "DARK", sectionTitle: "Title" });
	s.addText("Where should the money go?", { placeholder: "title" });
	s.addText("An agent that decides how a routing node's liquidity is deployed", { placeholder: "body" });

	// Motif: our node choosing between peers, top right.
	const cy = 0.95, ux = 7.0;
	circle(s, ux, cy, 0.55, "us", { fill: { color: C.accent1 }, line: { color: C.accent1, width: 0 }, fontSize: 12 });
	[[8.3, 0.45], [8.6, 0.95], [8.3, 1.45]].forEach(([px, py], i) => {
		arrow(s, ux + 0.28, cy, px - 0.2, py, { color: C.accent5, width: i === 1 ? 2.5 : 1, dashType: i === 1 ? "solid" : "dash" });
		circle(s, px, py, 0.36, "", { fill: { color: i === 1 ? C.accent2 : C.accent6 }, line: { color: i === 1 ? C.accent2 : C.accent6, width: 0 } });
	});
	text(s, "?", { x: 9.0, y: 0.65, w: 0.5, h: 0.6, fontSize: 36, bold: true, color: C.accent5, align: "center", valign: "middle" });

	text(s, "Hackathon prototype  ·  LDK Node  +  goose-agent (GDK)", { x: 0.6, y: 4.9, w: 8, h: 0.3, fontSize: 12, color: C.accent6 });

	s.addNotes("A Lightning routing node earns a fee on every payment it forwards, and it can only forward where it has capital. This project is an agent, built on the GDK over LDK Node, that decides where that capital should go and asks the operator before it moves any.");
}

// ---------------------------------------------------------------- slide 2: the problem

pres.addSection({ title: "The problem" });

{
	const s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "The problem" });
	s.addText("Two different jobs hide in “liquidity management”", { placeholder: "title" });
	text(s, "A routing node earns fees only if its capital sits in the right channels, and once it is in a channel it stays there for months.",
		{ x: 0.5, y: 1.05, w: 9.0, h: 0.3, fontSize: 12.5, color: C.text1 });

	const top = 1.6, h = 3.35, w = 4.35;

	// Left: balancing, a control problem.
	card(s, { x: 0.5, y: top, w, h }, "", {});
	circle(s, 0.95, top + 0.42, 0.5, "loop", { fill: { color: C.accent2 }, line: { color: C.accent2, width: 0 }, fontSize: 9 });
	text(s, "Keep a channel balanced", { x: 1.35, y: top + 0.22, w: w - 1.0, h: 0.4, fontSize: 16, bold: true, color: C.text2, valign: "middle" });
	text(s, "A control problem", { x: 0.7, y: top + 0.85, w: w - 0.4, h: 0.25, fontSize: 11, bold: true, color: C.accent2 });
	text(s, "One channel runs dry, another sits idle. The signal is clear and so is the response: nudge fees, or splice capital from one to the other.",
		{ x: 0.7, y: top + 1.12, w: w - 0.4, h: 0.75, fontSize: 11, valign: "top" });
	// A dry channel and a full one, with the move between them.
	text(s, "dry", { x: 0.7, y: top + 1.98, w: 0.4, h: 0.2, fontSize: 9, color: C.accent6 });
	channelBar(s, 1.15, top + 2.0, 1.3, 0.16, 8, { localColor: C.accent3 });
	arrow(s, 2.65, top + 2.48, 2.65, top + 2.08, { color: C.accent2, width: 1.5 });
	text(s, "splice", { x: 2.75, y: top + 2.17, w: 0.6, h: 0.2, fontSize: 9, italic: true, color: C.accent2 });
	text(s, "full", { x: 0.7, y: top + 2.38, w: 0.4, h: 0.2, fontSize: 9, color: C.accent6 });
	channelBar(s, 1.15, top + 2.4, 1.3, 0.16, 92, { localColor: C.accent2 });
	text(s, "A loop does this. We built that loop. It does not need a model.",
		{ x: 0.7, y: top + 2.78, w: w - 0.4, h: 0.45, fontSize: 11, italic: true, color: C.accent2, valign: "top" });

	// Right: deploying, a judgment problem.
	const rx = 5.15;
	card(s, { x: rx, y: top, w, h, fill: { color: C.text2 }, line: { color: C.text2, width: 0 } }, "", {});
	circle(s, rx + 0.45, top + 0.42, 0.5, "?", { fill: { color: C.accent1 }, line: { color: C.accent1, width: 0 }, fontSize: 14 });
	text(s, "Decide where capital goes", { x: rx + 0.85, y: top + 0.22, w: w - 1.0, h: 0.4, fontSize: 16, bold: true, color: C.background1, valign: "middle" });
	text(s, "A judgment problem", { x: rx + 0.2, y: top + 0.85, w: w - 0.4, h: 0.25, fontSize: 11, bold: true, color: C.accent1 });
	text(s, [
		{ text: "Which peers to open channels with, and how large.", options: { bullet: true, breakLine: true } },
		{ text: "Whether to buy inbound from an LSP.", options: { bullet: true, breakLine: true } },
		{ text: "Which channels to close and redeploy.", options: { bullet: true } },
	], { x: rx + 0.2, y: top + 1.12, w: w - 0.4, h: 1.0, fontSize: 11, color: C.accent5, valign: "top", paraSpaceAfter: 3 });
	text(s, "Many soft criteria over mixed data, and the answer is a proposal a person approves, because it locks money up for months.",
		{ x: rx + 0.2, y: top + 2.05, w: w - 0.4, h: 0.65, fontSize: 11, color: C.background1, valign: "top" });
	text(s, "That is the agent's job.",
		{ x: rx + 0.2, y: top + 2.85, w: w - 0.4, h: 0.3, fontSize: 13, bold: true, color: C.accent1 });

	s.addNotes("Two different jobs hide in 'liquidity management'. Keeping a channel balanced (nudging fees, splicing between channels when one side runs dry) is a control problem with a clear signal and a clear response. A deterministic loop does it well, and we have one. It does not justify a model.\n\nDeciding how liquidity is deployed is different: which peers to open channels with and how large, whether to buy inbound from an LSP, which channels to close and move elsewhere. The inputs are soft and mixed: graph position, fees, what payments actually failed and where they wanted to go, live liquidity from probes, and the operator's own goal in plain language. The output is a proposal with reasons, which a human approves because it commits capital for months. That is the job we gave to the agent.");
}

// ---------------------------------------------------------------- slide 3: what we built

pres.addSection({ title: "What we built" });

{
	const s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "What we built" });
	s.addText("What we built: three questions, then an action", { placeholder: "title" });

	// The investigation, left to right: three questions the tools answer, then the action.
	const top = 1.15, cw = 2.1, gap = 0.2, ch = 2.1;
	const cards = [
		["G", C.accent2, "Who could we connect to?",
			"The graph tool ranks the whole network by connectivity, capacity and fees and hands back a handful of candidates."],
		["$", C.accent4, "Where is demand we don't serve?",
			"The node's own ledger keeps every forward and every failed one, including where the failed payments wanted to go."],
		["?", C.accent6, "Is the liquidity really there?",
			"Probes send test payments along real paths. The graph says a channel exists; a probe says it can carry the amount."],
		["!", C.accent1, "Then act",
			"Open or close a channel, connect to a peer, set fees, splice capital in or out. Each one passes the gate first."],
	];
	cards.forEach(([ico, color, head, body], i) => {
		const x = 0.5 + i * (cw + gap);
		const last = i === cards.length - 1;
		card(s, { x, y: top, w: cw, h: ch, ...(last ? { fill: { color: C.text2 }, line: { color: C.text2, width: 0 } } : {}) }, "", {});
		circle(s, x + 0.38, top + 0.38, 0.46, ico, { fill: { color }, line: { color, width: 0 }, fontSize: 12 });
		text(s, head, { x: x + 0.15, y: top + 0.72, w: cw - 0.3, h: 0.6, fontSize: 13, bold: true, color: last ? C.background1 : C.text2, valign: "top" });
		text(s, body, { x: x + 0.15, y: top + 1.3, w: cw - 0.3, h: ch - 1.4, fontSize: 9.5, color: last ? C.accent5 : C.text1, valign: "top" });
		if (!last) arrow(s, x + cw + 0.02, top + 0.38, x + cw + gap - 0.02, top + 0.38, { color: C.accent6, width: 1.5 });
	});

	// Who does what: the tool narrows, the model chooses.
	const fy = 3.55;
	text(s, "The tool narrows. The model chooses.", { x: 0.5, y: fy, w: 4.2, h: 0.26, fontSize: 11.5, bold: true, color: C.accent2 });
	const funnel = [
		[3.9, C.accent6, C.background1, "hundreds of nodes in the graph"],
		[2.5, C.accent2, C.background1, "a handful the tool ranks highest"],
		[2.0, C.accent1, C.background1, "the model's picks, with reasons"],
	];
	const fcx = 0.5 + 4.2 / 2;
	funnel.forEach(([w, fill, color, label], i) => {
		const y = fy + 0.36 + i * 0.4;
		card(s, { x: fcx - w / 2, y, w, h: 0.3, fill: { color: fill }, line: { color: fill, width: 0 }, rectRadius: 0.04 },
			label, { fontSize: 9, color, align: "center", valign: "middle", margin: 0.02, bold: i === 2 });
	});

	// What was new, for the people judging.
	card(s, { x: 5.0, y: fy, w: 4.5, h: 1.55 }, [
		{ text: "New for this", options: { bold: true, fontSize: 11.5, color: C.text2, breakLine: true } },
		{ text: "In LDK Node: events that report failed forwards and every probe result.", options: { bullet: true, breakLine: true } },
		{ text: "In the agent: the graph tools, the demand ledger, and tools to open and close channels and connect to peers.", options: { bullet: true, breakLine: true } },
		{ text: "Already there: the background prober, fee changes, splicing.", options: { bullet: true } },
	], { fontSize: 9.5, color: C.text1, margin: 0.14, valign: "top", paraSpaceAfter: 3 });

	s.addNotes("This slide is the investigation the agent runs before it proposes anything, read left to right.\n\nWho could we connect to? Ranking a few hundred nodes by connectivity, capacity and fee policy is arithmetic, so a deterministic tool over LDK Node's gossip graph does it and returns a short list of candidates. A second graph tool gives the cheapest route cost from a candidate toward destinations the operator named, and a third profiles any node. The model reasons over the short list; it never reads the whole graph.\n\nWhere is demand we don't serve? The forwarding ledger is the node's own record of every forward and every failed forward. Failed forwards carry where the payment wanted to go, so the ledger shows demand we could not serve, not just traffic we did serve. The channel_overview, recent_forwards and forward_failures tools read it.\n\nIs the liquidity really there? The graph only says a channel exists. LDK Node's background prober, plus targeted probes the agent sends, show whether a path can actually carry an amount right now.\n\nThen act: open_channel, close_channel and connect_peer are new tools; set_channel_fees, splice_in and splice_out already existed. Every write goes through the policy gate on the next slide.\n\nWhat was new. In LDK Node: the ForwardFailed event (earlier in the project) and the ProbeSucceeded and ProbeFailed events (src/event.rs), so the agent sees every probe result. In the agent: the graph tools, the ledger with demand, and the open, close and connect tools. Already there: the background prober, set_channel_fees, splice_in, splice_out.");
}

// ---------------------------------------------------------------- slide 4: how the GDK runs it

pres.addSection({ title: "How it runs" });

{
	const s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "How it runs" });
	s.addText("How the GDK runs it: one loop, one memory", { placeholder: "title" });

	const steps = [
		["goal", C.accent6, "Goal", "The operator says what they want, in plain language."],
		["look", C.accent2, "Investigate", "Graph shortlists, the ledger, probes."],
		["plan", C.accent1, "Propose", "A plan with sizes and reasons."],
		["you", C.accent4, "Gate", "Opens and out-of-bounds writes wait for the operator."],
		["do", C.accent2, "Execute", "Typed tools act on LDK Node."],
		["ok?", C.accent6, "Verify", "Probe the new paths. Did it work?"],
		["mem", C.accent1, "Remember", "One conversation, kept across passes."],
	];
	const pitch = 9.0 / steps.length, cy = 1.55, d = 0.6;
	const cx = (i) => 0.5 + pitch * i + pitch / 2;
	steps.forEach(([ico, color, head, body], i) => {
		const x = cx(i);
		circle(s, x, cy, d, ico, { fill: { color }, line: { color, width: 0 }, fontSize: 10 });
		text(s, head, { x: x - pitch / 2, y: cy + 0.4, w: pitch, h: 0.3, fontSize: 13, bold: true, color: C.text2, align: "center" });
		text(s, body, { x: x - pitch / 2 + 0.08, y: cy + 0.7, w: pitch - 0.16, h: 0.7, fontSize: 9.5, color: C.text1, align: "center", valign: "top" });
		if (i < steps.length - 1) arrow(s, x + d / 2 + 0.04, cy, cx(i + 1) - d / 2 - 0.04, cy, { color: C.accent6, width: 1.5 });
	});
	// Gate: the operator's hand on the loop.
	text(s, "the operator approves", { x: cx(3) - 0.8, y: cy - 0.72, w: 1.6, h: 0.24, fontSize: 9.5, bold: true, color: C.accent4, align: "center" });
	arrow(s, cx(3), cy - 0.48, cx(3), cy - d / 2 - 0.04, { color: C.accent4, width: 1.5 });

	// Loop back: Remember -> Investigate, below the row.
	const ly = 3.2;
	arrow(s, cx(6), cy + 1.42, cx(6), ly, { color: C.accent6, width: 1.25, endArrowType: "none" });
	arrow(s, cx(6), ly, cx(1), ly, { color: C.accent6, width: 1.25, endArrowType: "none" });
	arrow(s, cx(1), ly, cx(1), cy + 1.46, { color: C.accent6, width: 1.25 });
	text(s, "every later review starts with the goal and earlier decisions already in memory",
		{ x: cx(1) + 0.3, y: ly + 0.06, w: cx(6) - cx(1) - 0.6, h: 0.26, fontSize: 9.5, italic: true, color: C.accent6, align: "center" });

	// Inside the GDK: three steps per pass, plus the persisted conversation.
	const gy = 3.75;
	text(s, "Inside the GDK, each pass is a three-step state machine", { x: 0.5, y: gy, w: 6, h: 0.26, fontSize: 11.5, bold: true, color: C.accent2 });
	const parts = [
		["Policy gate", "Inspects every tool request. In-bounds writes pass; channel opens and out-of-bounds writes wait for a yes. A no returns to the model as a tool error."],
		["Tool execution", "Typed async tools over LDK Node. The model gets structured results, not log output."],
		["Inference", "The model reasons over the results and decides the next call, or writes the plan."],
		["Persisted conversation", "Every message, result and decision is kept, so a review weeks later remembers why a channel exists."],
	];
	parts.forEach(([head, body], i) => {
		const x = 0.5 + i * 2.3, w = 2.15;
		card(s, { x, y: gy + 0.34, w, h: 1.05, fill: { color: C.background1 }, line: { color: C.accent2, width: 1 } },
			[{ text: head, options: { bold: true, color: C.text2, fontSize: 10.5, breakLine: true } },
			 { text: body, options: { color: C.text1, fontSize: 9 } }],
			{ margin: 0.1, valign: "top" });
		if (i < 2) arrow(s, x + w + 0.02, gy + 0.86, x + w + 0.13, gy + 0.86, { color: C.accent2, width: 1.25 });
	});

	s.addNotes("The agent is a goose-agent StateMachine with three operations per pass. The policy operation runs first and inspects every tool request the model made: reads pass, writes inside the operator's limits pass, and a channel open or any out-of-bounds write yields to the operator with the proposal and its reasons. If the operator declines, the call returns to the model as a tool error, so the model learns the decision and can adjust the plan instead of stalling. The tool operation runs the approved calls as typed async tools. The inference runner then sends the results to the model with the system prompt, and the model decides the next call or writes its plan.\n\nThere is one persisted conversation per node. Every message, tool result and operator decision is appended to it and replayed on restart. That is what makes later reviews useful: when the agent looks at a channel weeks after opening it, the goal and the reasons for that channel are already in its context.\n\nThe loop on the slide is the story: a goal in plain language, investigation through the graph, ledger and probe tools, a proposal with reasons, the gate, execution, verification by probing the new paths, and memory for the next pass.");
}

// ---------------------------------------------------------------- slide 5: the demo

pres.addSection({ title: "The demo" });

{
	const s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "The demo" });
	s.addText("The demo: give it a goal, approve its plan", { placeholder: "title" });

	// The real graph as a cloud, with our node, candidate peers and the named destinations inside.
	const cl = { x: 0.5, y: 1.2, w: 4.9, h: 3.65 };
	s.addShape(pres.ShapeType.ellipse, { x: cl.x, y: cl.y, w: cl.w, h: cl.h, fill: { color: C.background2 }, line: { color: C.background2, width: 0 } });
	text(s, "Mutinynet: a public signet with a real gossip graph", { x: cl.x, y: cl.y + 0.18, w: cl.w, h: 0.24, fontSize: 9.5, bold: true, color: C.accent6, align: "center" });

	const bg = [
		[1.3, 2.3], [1.6, 3.2], [1.25, 3.95], [2.1, 1.85], [2.0, 4.4], [2.7, 4.2], [3.3, 1.75], [3.9, 1.95],
		[4.4, 3.1], [3.8, 4.45], [4.7, 3.85], [2.55, 2.35], [3.25, 3.55], [1.85, 2.75], [4.2, 2.3], [3.0, 2.9],
	];
	const edges = [[0, 13], [13, 1], [1, 2], [2, 4], [4, 5], [5, 12], [12, 15], [15, 11], [11, 3], [3, 6], [6, 7], [7, 14], [14, 8], [8, 10], [10, 9], [9, 12], [11, 13], [15, 8]];
	edges.forEach(([a, b]) => arrow(s, bg[a][0], bg[a][1], bg[b][0], bg[b][1], { color: C.accent6, width: 0.6, endArrowType: "none" }));
	bg.forEach(([x, y]) => dot(s, x, y, 0.13, C.accent6));

	const US = { x: 2.55, y: 3.15 };
	const cand = [[1.75, 2.3], [3.35, 2.2], [2.05, 3.95]];
	const dest = [[4.35, 2.55], [4.3, 3.65]];
	// Proposed channels, dashed orange; the paths they buy toward the destinations, thin green.
	cand.forEach(([x, y]) => arrow(s, US.x, US.y, x, y, { color: C.accent1, width: 2, dashType: "dash", endArrowType: "none" }));
	arrow(s, cand[1][0], cand[1][1], dest[0][0], dest[0][1], { color: C.accent4, width: 1.25, endArrowType: "none" });
	arrow(s, cand[1][0], cand[1][1], bg[15][0], bg[15][1], { color: C.accent4, width: 1.25, endArrowType: "none" });
	arrow(s, bg[15][0], bg[15][1], dest[1][0], dest[1][1], { color: C.accent4, width: 1.25, endArrowType: "none" });
	arrow(s, cand[0][0], cand[0][1], bg[3][0], bg[3][1], { color: C.accent4, width: 1.25, endArrowType: "none" });
	arrow(s, bg[3][0], bg[3][1], bg[6][0], bg[6][1], { color: C.accent4, width: 1.25, endArrowType: "none" });
	arrow(s, bg[6][0], bg[6][1], dest[0][0], dest[0][1], { color: C.accent4, width: 1.25, endArrowType: "none" });

	cand.forEach(([x, y]) => circle(s, x, y, 0.38, "", { fill: { color: C.accent2 }, line: { color: C.accent2, width: 0 } }));
	dest.forEach(([x, y]) => circle(s, x, y, 0.38, "", { fill: { color: C.accent4 }, line: { color: C.accent4, width: 0 } }));
	circle(s, US.x, US.y, 0.6, "us", { fill: { color: C.accent1 }, line: { color: C.accent1, width: 0 }, fontSize: 13 });

	// Legend under the cloud.
	const leg = [[C.accent1, "our node"], [C.accent2, "candidate peers"], [C.accent4, "destinations you named"]];
	let lx = 0.55;
	leg.forEach(([color, label]) => {
		dot(s, lx + 0.08, 5.0, 0.16, color);
		const w = 0.3 + label.length * 0.062;
		text(s, label, { x: lx + 0.24, y: 4.89, w, h: 0.22, fontSize: 9, color: C.accent6 });
		lx += w + 0.3;
	});

	// The story, as four steps on the right.
	const x0 = 5.85, lineX = x0 + 0.2, y0 = 1.25, step = 0.95;
	s.addShape(pres.ShapeType.line, { x: lineX, y: y0 + 0.2, w: 0, h: step * 3, line: { color: C.accent6, width: 1.5 } });
	const story = [
		[C.accent6, "You type a goal",
			"“I process merchant payments. I want good routes to these two destinations.”"],
		[C.accent2, "The agent investigates and proposes",
			"A few channels to open, each with a size and a reason."],
		[C.accent4, "The gate holds the opens until you say yes",
			"You see the plan and the reasons before any money moves."],
		[C.accent1, "It opens them and probes to verify",
			"A later review spots a channel that earned nothing and proposes redeploying it."],
	];
	story.forEach(([color, head, body], i) => {
		const y = y0 + i * step;
		circle(s, lineX, y + 0.2, 0.4, String(i + 1), { fill: { color }, line: { color, width: 0 }, fontSize: 12 });
		text(s, head, { x: x0 + 0.55, y: y - 0.02, w: 3.1, h: 0.3, fontSize: 12, bold: true, color: C.text2 });
		text(s, body, { x: x0 + 0.55, y: y + 0.28, w: 3.1, h: 0.62, fontSize: 9.5, valign: "top", italic: i === 0, color: i === 0 ? C.text2 : C.text1 });
	});

	s.addNotes("This demo is planned and has not run yet; every shape below is what we expect, not a result.\n\nSetup: the node runs on Mutinynet, a public signet with real gossip (about 370 nodes and 930 channels, 30-second blocks), funded from the Mutinynet faucet. So the graph the agent reasons over is real, and probes hit real peers.\n\nExpected flow: the operator types a goal like 'I have 5M sats, I process merchant payments, I want good routes to these two destinations.' The agent calls the graph tools for a shortlist, checks route cost from each candidate toward the two destinations, probes a few paths, and proposes two or three channels with sizes and reasons. The policy gate holds the channel opens; the operator approves; the agent opens the channels and probes the new paths to verify they reach the destinations. In a later review, the agent notices a channel that forwarded nothing and proposes closing it and redeploying the capital.\n\nThe diagram: our node in orange, the candidate peers it proposes to open to in blue (dashed orange lines are the proposed channels), the destinations the operator named in green with the paths the new channels buy toward them, all inside the cloud of the real graph.");
}

// ---------------------------------------------------------------- slide 6: why an agent, and next

pres.addSection({ title: "Why an agent" });

{
	const s = pres.addSlide({ masterName: "DARK_CONTENT", sectionTitle: "Why an agent" });
	s.addText("Why an agent and not rules, and where this goes", { placeholder: "title" });

	const colW = 4.35, cy0 = 1.2, ch = 1.85;
	card(s, { x: 0.5, y: cy0, w: colW, h: ch, fill: { color: C.text1 }, line: { color: C.text1, width: 0 } }, [
		{ text: "Rules", options: { bold: true, fontSize: 18, color: C.background1, breakLine: true } },
		{ text: "One rule per criterion, each written and weighted by hand.", options: { bullet: true, breakLine: true } },
		{ text: "The weights are guesses that never meet the operator's actual goal.", options: { bullet: true, breakLine: true } },
		{ text: "The next situation has no rule.", options: { bullet: true } },
	], { fontSize: 12.5, color: C.background1, margin: 0.18, valign: "top", paraSpaceAfter: 5 });
	card(s, { x: 5.15, y: cy0, w: colW, h: ch, fill: { color: C.accent1 }, line: { color: C.accent1, width: 0 } }, [
		{ text: "The agent", options: { bold: true, fontSize: 18, color: C.text2, breakLine: true } },
		{ text: "Reads the shortlist, the demand, the probe results and your goal.", options: { bullet: true, breakLine: true } },
		{ text: "Writes a plan with a reason for every channel.", options: { bullet: true, breakLine: true } },
		{ text: "You approve it before money moves.", options: { bullet: true } },
	], { fontSize: 12.5, color: C.text2, margin: 0.18, valign: "top", paraSpaceAfter: 5 });

	text(s, "Underneath, the balance loop stays cheap and deterministic. The agent decides what that loop should be balancing.",
		{ x: 0.5, y: 3.18, w: 9.0, h: 0.3, fontSize: 12, italic: true, color: C.accent5 });

	text(s, "Next", { x: 0.5, y: 3.65, w: 2, h: 0.3, fontSize: 14, bold: true, color: C.background1 });
	const next = [
		["LSP", C.accent2, "Buy inbound", "LSPS1 purchases as part of the plan, through the same gate."],
		["wks", C.accent4, "Redeploy over weeks", "Close what never earns and move the capital to where demand is."],
		["goal", C.accent1, "Carry the goal", "The operator's goal lives across sessions; every review starts from it."],
	];
	next.forEach(([ico, color, head, body], i) => {
		const x = 0.5 + i * 3.05, w = 2.85;
		block(s, x, 4.1, w, ico, color, head, body, { headSize: 13, bodySize: 10.5, bodyH: 0.75, headColor: C.background1, bodyColor: C.accent5 });
	});

	s.addNotes("Why not a rules engine? Because every criterion that matters here (graph position, fee policy, demand we failed to serve, live liquidity, the operator's own goal) would need its own rule and its own weight, all set by hand, and the next situation would still have no rule. The model reads the shortlist the tools computed, the demand from the ledger, the probe results and the goal in plain language, and writes a justified plan. The operator reads the reasons and approves. The deterministic balance loop stays underneath; the agent decides what capital it should be balancing.\n\nNext steps: LSPS1 inbound purchases through the same gate; redeployment over weeks, closing channels that never earn and moving the capital toward demand; and carrying the operator's goal across sessions so every later review starts from it.");
}

// ---------------------------------------------------------------- write

(async () => {
	await pres.writeFile({ fileName: OUT });
	await applyTheme(OUT, THEME);
	console.log("wrote", OUT);
})().catch((e) => { console.error(e); process.exit(1); });
