# ChimeraMQ — BRANDING

> **Three Heads. One Binary. All Messages.**

**Project:** ChimeraMQ
**Domain:** chimeramq.com
**GitHub:** github.com/chimeramq/chimera
**Tagline (Primary):** "Three Heads. One Binary. All Messages."
**Tagline (Technical):** "Queue + Stream + Multi-Protocol. Zero Dependencies. Pure Go."

---

## 1. BRAND IDENTITY

### 1.1 Brand Story

In Greek mythology, the Chimera was a fire-breathing hybrid beast — part lion, part goat, part serpent — a creature so powerful that it took a hero riding Pegasus to defeat it. The Chimera was feared because it was unpredictable: you couldn't prepare for one head without the other two striking.

**ChimeraMQ embodies this mythology:**

| Chimera Head | MQ Function | What It Replaces |
|-------------|-------------|------------------|
| 🦁 **Lion** (strength, dominance) | Queue Engine | RabbitMQ, ActiveMQ, SQS |
| 🐐 **Goat** (endurance, sure-footed) | Stream Engine | Kafka, Pulsar, Redpanda |
| 🐍 **Serpent** (adaptability, stealth) | Protocol Engine | Protocol fragmentation |

The message is clear: **You don't need three systems. You need one beast.**

### 1.2 Brand Personality

- **Aggressive:** We don't coexist with legacy — we replace it
- **Confident:** Single binary does what three JVMs couldn't
- **Mythological:** Ancient power, modern engineering
- **Unapologetic:** Zero compromises, zero dependencies, zero excuses
- **Technical:** Developer-first, no enterprise fluff

### 1.3 Voice & Tone

- Direct, no hedging ("replaces" not "alternative to")
- Technical precision (specific numbers, benchmarks, comparisons)
- Mythology references woven naturally, never forced
- Turkish developer culture awareness (for X content)
- #NOFORKANYMORE alignment

---

## 2. COLOR PALETTE

### 2.1 Primary Colors

The palette draws from fire, mythology, and dark technical aesthetics.

| Color | Name | Hex | RGB | Usage |
|-------|------|-----|-----|-------|
| 🔴 | **Chimera Red** | `#C41E3A` | 196, 30, 58 | Primary brand, logo, CTAs |
| ⚫ | **Obsidian** | `#0D0D0D` | 13, 13, 13 | Backgrounds, dark mode |
| 🟡 | **Fire Gold** | `#D4A017` | 212, 160, 23 | Accents, highlights, fire theme |
| ⚪ | **Bone White** | `#F5F0E8` | 245, 240, 232 | Text on dark, light surfaces |

### 2.2 Secondary Colors (Head-Specific)

Each head has a signature color for diagrams, docs, and UI:

| Color | Name | Hex | Represents |
|-------|------|-----|------------|
| 🟠 | **Lion Amber** | `#E67E22` | Queue Engine (Lion Head) |
| 🟢 | **Goat Emerald** | `#1ABC9C` | Stream Engine (Goat Head) |
| 🟣 | **Serpent Violet** | `#8E44AD` | Protocol Engine (Serpent Head) |

### 2.3 Functional Colors

| Color | Name | Hex | Usage |
|-------|------|-----|-------|
| 🟢 | **Success** | `#27AE60` | Health, connected, ack'd |
| 🔴 | **Error** | `#E74C3C` | Failures, DLQ, disconnected |
| 🟡 | **Warning** | `#F39C12` | Lag, slow consumers |
| 🔵 | **Info** | `#3498DB` | Metrics, neutral info |

### 2.4 Gradient

Primary gradient for hero sections and banners:

```css
/* Chimera Fire Gradient */
background: linear-gradient(135deg, #C41E3A 0%, #D4A017 50%, #E67E22 100%);

/* Dark Chimera Gradient */
background: linear-gradient(135deg, #0D0D0D 0%, #1A0A0A 50%, #2D1A1A 100%);

/* Three Heads Gradient */
background: linear-gradient(120deg, #E67E22 0%, #C41E3A 33%, #8E44AD 66%, #1ABC9C 100%);
```

---

## 3. TYPOGRAPHY

### 3.1 Primary Font

**Inter** — clean, technical, excellent readability at all sizes.

- Headings: Inter Bold (700)
- Body: Inter Regular (400)
- Code/Technical: Inter Medium (500)

### 3.2 Monospace Font

**JetBrains Mono** — for code samples, CLI output, terminal aesthetics.

### 3.3 Display Font (Logo/Tagline)

**Cinzel** — Roman/Greek inspired serif, perfect for mythological branding.

- Logo text: Cinzel Bold
- Tagline: Cinzel Regular

### 3.4 Type Scale

```
Hero Title:     48px / Cinzel Bold / Chimera Red
Section Title:  32px / Inter Bold / Bone White
Subsection:     24px / Inter Bold / Bone White
Body:           16px / Inter Regular / Bone White (80% opacity)
Caption:        14px / Inter Regular / Bone White (60% opacity)
Code:           14px / JetBrains Mono / Fire Gold
```

---

## 4. LOGO CONCEPTS

### 4.1 Primary Logo — Three-Headed Silhouette

**Description:** A stylized three-headed beast silhouette in profile view. Three distinct heads emerge from a single body/neck:
- Left head: Lion (angular mane, jaw open)
- Center head: Goat (curved horns, upward)
- Right head: Serpent (hooded, tongue out)

The body forms a subtle hexagonal shape (node/cluster metaphor). The silhouette is bold, geometric, minimal — works at 16px favicon size.

**Color Treatment:**
- Full color: Each head in its signature color (Amber, Red, Violet) merging at the body
- Monochrome: All Chimera Red on Obsidian
- Inverted: Bone White on Obsidian

### 4.2 Alternate Logo — Three Flames

**Description:** Three stylized flames arranged in a triangle formation, each flame representing a head:
- Left flame: Lion Amber (tallest)
- Center flame: Chimera Red (medium)
- Right flame: Serpent Violet (coiling)

Flames merge at the base into a single fire point. Geometric/low-poly style.

### 4.3 Icon/Favicon — Triple Eye

**Description:** Three eyes arranged in a triangle (reminiscent of the three-headed beast watching). Each eye a different signature color. Works perfectly at small sizes. Eyes are stylized — angular, not realistic.

### 4.4 Wordmark

```
CHIMERA|MQ
```

- "CHIMERA" in Cinzel Bold, Chimera Red
- "|" separator bar, Fire Gold
- "MQ" in Inter Bold, Bone White (or 60% opacity)
- The "|" also subtly references a message queue pipeline

### 4.5 Logo Usage Rules

- Minimum clear space: 1x logo height on all sides
- Minimum size: 24px height for icon, 120px for wordmark
- Never stretch, rotate, or apply effects
- On dark backgrounds: use monochrome white or full color
- On light backgrounds: use Chimera Red or full color

---

## 5. VISUAL LANGUAGE

### 5.1 Architecture Diagrams

- Dark background (#0D0D0D)
- Head-specific colors for each engine section
- Glowing connection lines (Fire Gold at 40% opacity)
- Rounded rectangles for components
- Subtle grid pattern overlay (5% opacity)

### 5.2 Code Blocks

```
Background: #1A1A2E
Border-left: 3px solid #C41E3A
Font: JetBrains Mono 14px
Syntax highlighting: Dracula-inspired but with Chimera colors
  - Keywords: Chimera Red
  - Strings: Fire Gold
  - Functions: Goat Emerald
  - Comments: Bone White 40%
  - Types: Serpent Violet
```

### 5.3 Comparison Tables

| Feature | ChimeraMQ | Kafka | RabbitMQ |
|---------|-----------|-------|----------|
| ✅ Has  | Goat Emerald text | — | — |
| ❌ Missing | — | Chimera Red text | Chimera Red text |

### 5.4 Iconography

Use Lucide icons or custom minimal line icons. Three-line motif (representing three heads) appears as decorative element in documentation headers.

---

## 6. WEBSITE (chimeramq.com)

### 6.1 Landing Page Structure

```
[Hero Section]
  - Dark gradient background with subtle fire particle animation
  - Three-headed silhouette logo, large
  - "Three Heads. One Binary. All Messages."
  - "Queue + Stream + Multi-Protocol. Zero Dependencies. Pure Go."
  - [Get Started] [GitHub] buttons

[The Beast Section]
  - Three columns, each head:
    🦁 Lion Head — Queue Engine
    "Competing consumers, ack/nack, DLQ, delayed messages, priority queues"
    
    🐐 Goat Head — Stream Engine
    "Partitioned log, consumer groups, replay, compaction, windowing"
    
    🐍 Serpent Head — Protocol Engine
    "Native + AMQP 1.0 + MQTT + WebSocket + HTTP — all on one port"

[Why ChimeraMQ Section]
  - Comparison table vs Kafka, RabbitMQ, Pulsar, NATS
  - Binary size, startup time, memory usage comparisons
  - "One go install. That's it."

[Architecture Section]
  - Animated (or SVG) architecture diagram
  - Tiered storage visualization (Hot → Warm → Cold)
  - Cluster topology (Raft + Gossip)

[Quick Start Section]
  - 4 terminal commands: install, start, create topic, produce/consume
  - Copy-pasteable

[Footer]
  - GitHub, Docs, Discord/Community links
  - ECOSTACK TECHNOLOGY OÜ
  - Apache 2.0
```

### 6.2 Technical: Next.js Static Export

- Next.js 15 + Tailwind CSS v4 + shadcn/ui
- Static export (output: 'export') for CDN deployment
- Dark mode only (brand identity)
- Three.js or canvas animation for hero fire particles
- SEO optimized (meta tags, OpenGraph, Twitter Cards)

---

## 7. GITHUB PRESENCE

### 7.1 Repository Banner

**Size:** 1280x640px
**Content:**
- Obsidian background with subtle fire gradient at bottom
- Three-headed silhouette logo centered
- "ChimeraMQ" wordmark below
- "Three Heads. One Binary. All Messages." tagline
- Small "Pure Go • Zero Dependencies • Apache 2.0" bottom text

### 7.2 Social Preview (OpenGraph)

**Size:** 1200x630px
Same as repository banner but optimized for social sharing. Larger text, more contrast.

### 7.3 Badges (README)

```markdown
[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?style=flat-square&logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache%202.0-C41E3A?style=flat-square)](LICENSE)
[![Build](https://img.shields.io/github/actions/workflow/status/chimeramq/chimera/ci.yml?style=flat-square)](https://github.com/chimeramq/chimera/actions)
[![Release](https://img.shields.io/github/v/release/chimeramq/chimera?style=flat-square&color=D4A017)](https://github.com/chimeramq/chimera/releases)
```

---

## 8. NANO BANANA 2 INFOGRAPHIC PROMPTS

### 8.1 Product Launch Infographic

```
Create a vertical product infographic (1080x1920 Instagram story / X post format) for "ChimeraMQ" — a unified message queue and event streaming platform.

HEADER AREA:
- Deep black/obsidian background (#0D0D0D) with subtle dark red gradient glow at top
- Large stylized three-headed beast silhouette (lion + goat + serpent) as a geometric/low-poly icon in crimson red (#C41E3A) with gold (#D4A017) accent highlights
- Bold title "ChimeraMQ" in elegant serif font (Cinzel style), crimson red
- Subtitle below: "Three Heads. One Binary. All Messages." in clean sans-serif, bone white (#F5F0E8)

THREE COLUMNS SECTION (the three heads):
- Three vertical cards/columns side by side, each with:
  LEFT CARD — "🦁 LION HEAD" header in amber (#E67E22)
    "Queue Engine"
    Bullet icons: Competing Consumers • Ack/Nack • Dead-Letter Queue • Priority • Delayed Messages
    Bottom label: "Replaces: RabbitMQ, ActiveMQ, SQS"

  CENTER CARD — "🐐 GOAT HEAD" header in emerald (#1ABC9C)
    "Stream Engine"
    Bullet icons: Partitioned Log • Consumer Groups • Replay • Compaction • Windowing
    Bottom label: "Replaces: Kafka, Pulsar, Redpanda"

  RIGHT CARD — "🐍 SERPENT HEAD" header in violet (#8E44AD)
    "Protocol Engine"
    Bullet icons: Native Binary • AMQP 1.0 • MQTT 3.1/5.0 • WebSocket • HTTP/REST
    Bottom label: "All Protocols, One Port"

COMPARISON SECTION:
- Title: "The Beast vs. The Legacy" in gold
- Horizontal comparison bars or table:
  ChimeraMQ: Single binary, 25MB, <1s cold start, Go
  vs Kafka: JVM + ZooKeeper, 300MB+, 30s+ start, Java
  vs RabbitMQ: Erlang runtime, 150MB+, split-brain issues, Erlang
  vs Pulsar: JVM + BookKeeper + ZooKeeper, 500MB+, Java
- ChimeraMQ row highlighted in crimson red, others in muted gray

FEATURES STRIP:
- Horizontal icon strip with small icons and labels:
  Zero Dependencies • Tiered Storage (Hot→Warm→Cold) • Embedded Raft + Gossip
  Schema Registry • WASM Transforms • Stream Processing • MCP Server

FOOTER:
- "go install github.com/chimeramq/chimera@latest" in monospace font, gold text on dark card
- "chimeramq.com" and GitHub icon
- "#NOFORKANYMORE" hashtag in small crimson text
- "ECOSTACK TECHNOLOGY OÜ" in very small bone white text

STYLE NOTES:
- Ultra dark premium aesthetic, like a luxury tech product reveal
- Subtle fire/ember particle effects in background
- All text high contrast and readable
- Cards have thin border (1px) in respective head colors with very subtle glow
- Overall mood: powerful, mythological, premium open-source
- NO clip art, NO cartoon style — sleek, modern, geometric
```

### 8.2 Architecture Infographic

```
Create a vertical technical architecture infographic (1080x1920) for "ChimeraMQ" message queue system.

HEADER:
- Black background (#0D0D0D)
- "ChimeraMQ Architecture" in crimson red (#C41E3A) serif font
- Subtitle: "How The Beast Works" in gold (#D4A017)

MAIN DIAGRAM (top-down flow):
- Show a layered architecture diagram with these tiers, flowing top to bottom:

TIER 1 — PROTOCOL LAYER (top, violet #8E44AD accent):
  Box labeled "Protocol Multiplexer" with 5 input arrows from left:
  "AMQP 1.0" | "MQTT" | "WebSocket" | "HTTP/REST" | "Chimera Native"
  All arrows converge into single "Auto-Detect" box
  Label: "Single TCP Port • TLS • Auto Protocol Detection"

TIER 2 — ROUTING LAYER (crimson #C41E3A accent):
  Box: "Message Router"
  Sub-labels: "Exchanges • Topic Routing • Partition Resolution • Murmur3 Hashing"
  Arrow splits into two paths downward:

TIER 3 — DUAL ENGINE (split into two side-by-side boxes):
  LEFT box in amber (#E67E22): "🦁 Queue Engine"
    "Round-Robin Dispatch • Prefetch • Ack/Nack • Visibility Timeout • DLQ • Priority • Delayed"
  RIGHT box in emerald (#1ABC9C): "🐐 Stream Engine"
    "Partitions • Consumer Groups • Offsets • Replay • Compaction • Windowing • Joins"
  CENTER label between them: "UNIFIED MODE — Same Data, Dual Consumption"
  Dotted line connecting both boxes at bottom

TIER 4 — STORAGE (gold #D4A017 accent):
  Three horizontal layers stacked:
  TOP:    "🔥 HOT — Memory-Mapped Log Segments" (bright, warm colors)
          "mmap • sendfile zero-copy • 256MB segments • sparse index"
  MIDDLE: "💾 WARM — LSM-Tree" (medium brightness)
          "SSTables • Bloom filters • Compaction • Indexed"
  BOTTOM: "🧊 COLD — Compressed Archives" (cool, muted colors)
          "Zstd dictionary • 1GB archives • Decompress on read"
  Arrows showing data flow: Hot → Warm → Cold with time labels

TIER 5 — CLUSTER FABRIC (bottom, dark with subtle glow):
  Two sub-sections:
  LEFT:  "Raft Consensus (Control Plane)" — "Metadata • Schemas • ACLs • Assignments"
  RIGHT: "Gossip / SWIM (Data Plane)" — "Discovery • Health • Failure Detection • Metrics"

FOOTER:
- "Zero External Dependencies • Single Binary • Pure Go"
- "chimeramq.com" with GitHub icon
- Small ECOSTACK TECHNOLOGY OÜ text

STYLE:
- Dark premium tech aesthetic
- Each tier separated by subtle horizontal divider lines with glow
- Arrows are clean, thin, with slight glow in respective tier colors
- Connection lines between tiers use gold at low opacity
- Boxes have rounded corners, thin colored borders, dark fill (#1A1A1A)
- Overall: clean, readable, impressive technical diagram
```

### 8.3 "Why ChimeraMQ" Comparison Infographic

```
Create a vertical comparison infographic (1080x1920) titled "Why ChimeraMQ?" comparing message queue systems.

HEADER:
- Black background (#0D0D0D)
- Three-headed beast icon in crimson red, small, top center
- "Why ChimeraMQ?" in large crimson red serif font
- "Stop juggling three systems. Deploy one beast." in gold (#D4A017)

SECTION 1 — "THE PROBLEM" (top third):
- Title: "The Messaging Mess" in bone white
- Four problem cards in a 2x2 grid, each with muted gray (#333) background:
  
  Card 1 — Kafka icon placeholder
  "Apache Kafka"
  "JVM + ZooKeeper/KRaft"
  "Stream only — need RabbitMQ for queues"
  "300MB+ RAM idle"
  Status: 🔴 HEAVY

  Card 2 — RabbitMQ icon placeholder
  "RabbitMQ"
  "Erlang/OTP runtime"
  "Queue only — need Kafka for streams"
  "Split-brain cluster nightmares"
  Status: 🔴 FRAGILE

  Card 3 — Pulsar icon placeholder
  "Apache Pulsar"
  "JVM + BookKeeper + ZooKeeper"
  "Three separate systems to manage"
  "500MB+ just to start"
  Status: 🔴 COMPLEX

  Card 4 — NATS icon placeholder
  "NATS"
  "Go (lightweight)"
  "JetStream feels bolted on"
  "Limited queue semantics"
  Status: 🟡 INCOMPLETE

SECTION 2 — "THE SOLUTION" (middle, highlighted):
- Full-width card with crimson red border glow
- "ChimeraMQ" title large, crimson
- "Pure Go • Single Binary • 25MB • <1 Second Start"
- Three mini-columns inside:
  🦁 "Full Queue" | 🐐 "Full Stream" | 🐍 "All Protocols"
- Status: 🟢 COMPLETE

SECTION 3 — COMPARISON TABLE (bottom):
- Title: "Head-to-Head" in gold
- Clean comparison table with these rows:
  
  | Feature | ChimeraMQ | Kafka | RabbitMQ | NATS |
  | Language | Go ✅ | Java ❌ | Erlang ❌ | Go ✅ |
  | Queue Semantics | ✅ Full | ❌ No | ✅ Full | 🟡 Basic |
  | Stream Semantics | ✅ Full | ✅ Full | ❌ No | 🟡 JetStream |
  | Unified Mode | ✅ Yes | ❌ No | ❌ No | ❌ No |
  | Multi-Protocol | ✅ 5 protocols | ❌ Kafka only | 🟡 AMQP only | ❌ NATS only |
  | Single Binary | ✅ Yes | ❌ No | ❌ No | ✅ Yes |
  | Zero Dependencies | ✅ Yes | ❌ JVM+ZK | ❌ Erlang | ✅ Yes |
  | Tiered Storage | ✅ Hot/Warm/Cold | 🟡 Tiered (plugin) | ❌ No | ❌ No |
  | Schema Registry | ✅ Built-in | 🟡 Separate | ❌ Plugin | ❌ No |
  | WASM Transforms | ✅ Built-in | ❌ No | ❌ No | ❌ No |

- ChimeraMQ column highlighted with subtle crimson background
- ✅ in emerald green, ❌ in muted red, 🟡 in gold

FOOTER:
- "go install github.com/chimeramq/chimera@latest" monospace gold
- "chimeramq.com"
- "#NOFORKANYMORE"

STYLE:
- Same dark premium aesthetic
- Table rows alternate very subtle gray tones
- ChimeraMQ column stands out with slight red tint
- Clean, data-dense, developer-oriented
```

### 8.4 Quick Start / DX Infographic

```
Create a vertical developer experience infographic (1080x1920) for "ChimeraMQ" showing how easy it is to get started.

HEADER:
- Black background (#0D0D0D)
- Small three-headed icon in crimson
- "ChimeraMQ in 60 Seconds" in crimson serif font
- "From zero to pub/sub in four commands" in gold

STEP 1 — INSTALL:
- Terminal window mockup (dark theme, rounded corners, traffic light dots)
- Command: "$ go install github.com/chimeramq/chimera@latest"
- Below: "Single binary. 25MB. Zero dependencies."
- Small icon: download arrow in emerald

STEP 2 — START:
- Terminal window:
- Command: "$ chimera server"
- Output lines:
  "ChimeraMQ v0.1.0 starting..."
  "Protocol listener: 0.0.0.0:5672 (chimera|amqp|mqtt|ws)"
  "Admin API: 0.0.0.0:9090"
  "Dashboard: http://localhost:9090/ui"
  "Ready in 47ms 🔥"
- Small icon: rocket in amber

STEP 3 — CREATE TOPIC:
- Terminal window:
- Command: "$ chimera topic create --name orders --mode unified --partitions 8"
- Output: "Topic 'orders' created (unified mode, 8 partitions)"
- Below: "Unified = Queue + Stream on same topic"
- Small icon: plus circle in violet

STEP 4 — PUBLISH & CONSUME:
- Terminal split into two panes side by side:
- LEFT pane (Producer):
  "$ chimera produce --topic orders --message '{"id": 1, "item": "coffee"}'"
  "Published: partition=3 offset=0"
- RIGHT pane (Consumer):
  "$ chimera consume --topic orders --follow"
  '{"id": 1, "item": "coffee"}'
- Small icon: arrows in emerald

BONUS SECTION — "But wait, there's more...":
- Small cards showing:
  "Connect MQTT devices" → same topic
  "AMQP 1.0 enterprise" → same topic
  "WebSocket frontend" → same topic
  "HTTP webhook" → same topic
  All arrows pointing to center "orders" topic
- Label: "5 protocols, 1 topic, 0 configuration"

FOOTER:
- "Read the docs: chimeramq.com/docs"
- GitHub star button mockup
- "Built with 🔥 by ECOSTACK TECHNOLOGY OÜ"
- "#NOFORKANYMORE"

STYLE:
- Terminal windows are the hero visual element
- Monospace font (JetBrains Mono style) for all terminal content
- Syntax highlighting in terminal: commands in bone white, output in green/gold
- Steps connected by subtle vertical dotted line (gold, low opacity)
- Each step numbered with large circled number in crimson
- Clean, developer-focused, no fluff
```

### 8.5 X Article Header Image

```
Create a wide header image (1600x900) for a technical article about ChimeraMQ.

COMPOSITION:
- Full dark background (#0D0D0D) with very subtle dark red radial gradient from center
- Center: Large three-headed beast silhouette in geometric/low-poly style
  - Lion head (left) breathing fire particles in amber (#E67E22)
  - Goat head (center) with glowing horns in crimson (#C41E3A)
  - Serpent head (right) with violet (#8E44AD) energy/lightning
- The beast stands on a stylized circuit board / data stream pattern (gold lines, very subtle)
- Below beast: "ChimeraMQ" wordmark in Cinzel Bold, crimson red, large
- Below wordmark: "Three Heads. One Binary. All Messages." in Inter, bone white, medium
- Bottom left corner: small "chimeramq.com" in bone white 50%
- Bottom right corner: small Go gopher silhouette in Go blue (#00ADD8) 30% opacity

STYLE:
- Dramatic, cinematic lighting from below (fire glow)
- The three heads each emit their signature color as ambient light
- Geometric/angular art style — NOT cartoon, NOT realistic
- Think: dark fantasy game title screen meets technical product
- High contrast, text must be readable when used as article header
- Widescreen format optimized for X (Twitter) article cards
```

---

## 9. X (TWITTER) CONTENT TEMPLATES

### 9.1 Launch Announcement Post (Turkish)

```
🔥 ChimeraMQ — Üç Başlı Canavar Uyanıyor

Kafka + RabbitMQ + Pulsar = tek Go binary.

🦁 Queue Engine → RabbitMQ'yu replace eder
🐐 Stream Engine → Kafka'yı replace eder  
🐍 Protocol Engine → AMQP + MQTT + WebSocket + HTTP hepsi tek portta

Özellikler:
• Zero external dependency (sadece Go stdlib)
• Unified Mode: aynı topic hem queue hem stream
• Tiered Storage: Hot (mmap) → Warm (LSM-Tree) → Cold (zstd)
• Built-in Schema Registry, WASM transforms, Stream Processing
• Embedded Raft + Gossip clustering

25MB binary. <1 saniye cold start. 1M+ msg/sec.

Java/Erlang/ZooKeeper zincirleri kopuyor.

github.com/chimeramq/chimera
chimeramq.com

#ChimeraMQ #Go #Golang #MessageQueue #EventStreaming #Kafka #RabbitMQ #NOFORKANYMORE #OpenSource
```

### 9.2 Thread Opener (Turkish)

```
THREAD 🧵

Neden yeni bir Message Queue yazıyorum?

Kafka = Java + ZooKeeper. "Basit pub/sub" için 2GB RAM.
RabbitMQ = Erlang. Split-brain. Cluster yönetimi kabusları.
Pulsar = Java + BookKeeper + ZooKeeper. 3 ayrı sistem deploy et.
NATS = Go ama JetStream sonradan eklenti hissi veriyor.

Ve en önemlisi: HIÇBIRI queue + stream'i tek binary'de unify etmiyor.

Ya queue kullan (RabbitMQ), ya stream kullan (Kafka).

İkisini istiyorsan? İki sistem deploy et, ikisini yönet, ikisine monitor kur.

2025'te bu kabul edilebilir mi?

ChimeraMQ diyor ki: hayır. ↓
```

### 9.3 Technical Deep Dive Posts

**Post: Unified Mode**
```
ChimeraMQ'nun killer feature'ı: Unified Mode

Aynı topic'e yazılan mesajlar:
→ Stream consumer offset ile okur (Kafka-style)
→ Queue consumer competing dispatch ile alır (RabbitMQ-style)
→ Data tek kopya, duplication yok

Nasıl?

Stream consumer: committed offset takip eder
Queue consumer: ack bitmap takip eder

İkisi de aynı hot segment'ten okur. Zero-copy.

Bu demek ki: event sourcing + work queue + real-time stream AYNI topic üzerinde.

Kafka bunu yapamaz. RabbitMQ bunu yapamaz.
Chimera yapabilir. Çünkü Chimera üç başlı 🔥
```

**Post: Protocol Multiplexer**
```
Tek TCP portu. Beş farklı protokol.

ChimeraMQ'nun Protocol Multiplexer'ı ilk byte'lara bakarak otomatik tespit ediyor:

0x16 0x03 → TLS (unwrap, tekrar detect)
"AMQP" → AMQP 1.0 handler
0x10 → MQTT CONNECT
"GET " → HTTP/WebSocket
"CHMR" → Chimera native protocol

Yani:
• IoT cihazların MQTT ile bağlanır
• Enterprise sistemler AMQP 1.0 ile konuşur
• Frontend WebSocket ile subscribe olur
• Monitoring HTTP/REST ile sorgular
• High-perf client native binary protocol kullanır

Hepsi aynı topic'e, aynı port'tan. Config: sıfır.
```

---

## 10. PRESENTATION TEMPLATES

### 10.1 Conference Talk Title Slide

```
[Dark background, three-headed silhouette]

ChimeraMQ
Three Heads. One Binary. All Messages.

Why I Built a Kafka + RabbitMQ + Pulsar Replacement in Pure Go

Ersin Koç
ECOSTACK TECHNOLOGY OÜ
```

### 10.2 Key Slides

1. "The Problem" — fragmented messaging landscape
2. "The Beast" — three heads explanation
3. "Unified Mode" — the killer feature diagram
4. "Storage Tiers" — Hot/Warm/Cold visualization
5. "Protocol Magic" — multiplexer demo
6. "Benchmarks" — performance comparison
7. "Demo" — live demo
8. "Roadmap" — 7 phases
9. "Get Involved" — GitHub, community

---

## 11. COMMUNITY & SOCIAL

### 11.1 GitHub Organization

- **Org name:** `chimeramq`
- **Org avatar:** Three-headed silhouette icon, crimson on transparent
- **Repo:** `chimeramq/chimera`
- **Topics:** `message-queue`, `event-streaming`, `golang`, `pub-sub`, `amqp`, `mqtt`, `kafka-alternative`, `rabbitmq-alternative`

### 11.2 Social Accounts

- **X/Twitter:** @chimeramq
- **Discord:** discord.gg/chimeramq (future)
- **Domain:** chimeramq.com

### 11.3 Hashtags

Primary: `#ChimeraMQ`
Secondary: `#NOFORKANYMORE` `#PureGo` `#ZeroDeps`
Community: `#OpenSource` `#Golang` `#MessageQueue` `#EventStreaming`

---

## 12. ASSET CHECKLIST

### Immediate Need (Phase 1 Launch)

- [ ] Logo SVG (primary three-headed silhouette)
- [ ] Logo SVG (wordmark)
- [ ] Favicon (triple eye, 32x32 + 16x16 + ICO)
- [ ] GitHub repository banner (1280x640)
- [ ] Social preview / OpenGraph image (1200x630)
- [ ] README badges configured
- [ ] Product launch infographic (Nano Banana 2)
- [ ] Architecture infographic (Nano Banana 2)
- [ ] X launch post (Turkish)
- [ ] chimera.yaml.example with branding colors in comments

### Future (Phase 2+)

- [ ] Comparison infographic (Nano Banana 2)
- [ ] Quick start infographic (Nano Banana 2)
- [ ] X article header image (Nano Banana 2)
- [ ] Conference slide deck template
- [ ] Website (chimeramq.com) design and build
- [ ] Video intro animation (three heads emerging)
- [ ] Sticker designs (three-headed beast, "I replaced Kafka")
- [ ] T-shirt design ("Three Heads. One Binary.")
