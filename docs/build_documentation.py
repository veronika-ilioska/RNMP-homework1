"""Build the four-page Macedonian submission PDF from measured aggregate results."""

import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Flowable, PageBreak, Paragraph, SimpleDocTemplate, Table, TableStyle


ROOT = Path(__file__).resolve().parent.parent
INK = colors.HexColor("#182740")
BLUE = colors.HexColor("#4e65d9")
MUTED = colors.HexColor("#6d7f98")
LIGHT = colors.HexColor("#f1f4fb")


def register_fonts():
    candidates = [("C:/Windows/Fonts/arial.ttf", "C:/Windows/Fonts/arialbd.ttf"),
                  ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                   "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")]
    for normal, bold in candidates:
        if Path(normal).is_file() and Path(bold).is_file():
            pdfmetrics.registerFont(TTFont("Body", normal))
            pdfmetrics.registerFont(TTFont("BodyBold", bold))
            pdfmetrics.registerFontFamily("Body", normal="Body", bold="BodyBold", italic="Body", boldItalic="BodyBold")
            return
    raise RuntimeError("Install Arial or DejaVu Sans to render Macedonian Cyrillic correctly")


def dataset_metadata(path):
    target = ROOT / "docs/dataset-metadata.json"
    if not path.is_file():
        return json.loads(target.read_text(encoding="utf-8"))
    users, types, times = set(), Counter(), []
    with gzip.open(path, "rt", encoding="utf-8") as source:
        for line in source:
            if line.strip():
                row = json.loads(line)
                users.add(str(row["actor"]["id"]))
                types[row["type"]] += 1
                times.append(row["created_at"])
    result = {"filename": path.name, "origin": "Provided during class; filename supplied by the student",
              "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "compressed_bytes": path.stat().st_size,
              "events": len(times), "users": len(users), "event_types": dict(types),
              "first_event_utc": min(times), "last_event_utc": max(times), "key": "actor.id"}
    target.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    return result


class Architecture(Flowable):
    def __init__(self):
        super().__init__()
        self.width, self.height = 507, 174

    def draw(self):
        canvas = self.canv

        def box(x, y, width, title, detail):
            canvas.setFillColor(LIGHT)
            canvas.setStrokeColor(colors.HexColor("#dbe3f4"))
            canvas.roundRect(x, y, width, 45, 6, fill=1, stroke=1)
            canvas.setFillColor(INK)
            canvas.setFont("BodyBold", 10)
            canvas.drawCentredString(x + width / 2, y + 27, title)
            canvas.setFont("Body", 8)
            canvas.setFillColor(MUTED)
            canvas.drawCentredString(x + width / 2, y + 12, detail)

        def arrow(x1, y1, x2, y2):
            canvas.setStrokeColor(BLUE)
            canvas.setLineWidth(1.2)
            canvas.line(x1, y1, x2, y2)
            canvas.line(x2, y2, x2 - 5, y2 + 3)
            canvas.line(x2, y2, x2 - 5, y2 - 3)

        box(0, 120, 133, "Python producer", "JSON + actor.id + event time")
        box(174, 120, 133, "Kafka input", "github-users-repo1")
        box(348, 120, 159, "Java / Apache Flink", "keyBy(userId), 2 модула")
        arrow(135, 142, 170, 142)
        arrow(310, 142, 344, 142)
        box(168, 56, 160, "Аналитички резултати", "github-user-sessions-repo1")
        box(347, 56, 160, "Event-driven резултати", "github-user-alerts-repo1")
        canvas.setStrokeColor(BLUE)
        canvas.line(426, 120, 426, 107)
        canvas.line(248, 107, 426, 107)
        canvas.line(248, 107, 248, 102)
        canvas.line(426, 107, 426, 102)
        canvas.setFillColor(INK)
        canvas.setFont("BodyBold", 9)
        canvas.drawString(0, 24, "Локален dashboard")
        canvas.setFont("Body", 8.5)
        canvas.setFillColor(MUTED)
        canvas.drawString(0, 8, "Независно ги чита трите Kafka теми; HTTP приказ на 127.0.0.1:8501.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=Path.home() / "Downloads/2026-10-15-15.json.gz")
    parser.add_argument("--output", type=Path, default=ROOT / "output/pdf/project-documentation.pdf")
    args = parser.parse_args()
    register_fonts()
    data = dataset_metadata(args.dataset)
    report = json.loads((ROOT / "reports/offset-comparison.json").read_text(encoding="utf-8"))
    first, latest = report["earliest"], report["latest"]
    body = ParagraphStyle("BodyTextMK", fontName="Body", fontSize=10, leading=15, textColor=INK,
                          spaceAfter=9, alignment=TA_LEFT)
    title = ParagraphStyle("TitleMK", parent=body, fontName="BodyBold", fontSize=25, leading=30, spaceAfter=12)
    section = ParagraphStyle("SectionMK", parent=body, fontName="BodyBold", fontSize=13, leading=19,
                             spaceBefore=13, spaceAfter=7)
    small = ParagraphStyle("SmallMK", parent=body, fontSize=8.3, leading=12, textColor=MUTED)
    code = ParagraphStyle("CodeMK", parent=body, fontName="Courier", fontSize=8.2, leading=12,
                          backColor=LIGHT, borderPadding=10, spaceBefore=6, spaceAfter=15)
    story = []

    def p(text, style=body):
        return Paragraph(text, style)

    def add(text, style=body):
        story.append(p(text, style))

    def heading(text):
        add(text, section)

    def table(rows, widths):
        items = [[p(escape(str(cell)), body if index else small) for cell in row] for index, row in enumerate(rows)]
        widget = Table(items, colWidths=widths, hAlign="LEFT")
        widget.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), LIGHT),
                                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                                    ("LEFTPADDING", (0, 0), (-1, -1), 11),
                                    ("RIGHTPADDING", (0, 0), (-1, -1), 11),
                                    ("TOPPADDING", (0, 0), (-1, -1), 8),
                                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                                    ("LINEBELOW", (0, 0), (-1, -1), .4, colors.HexColor("#e2e7f0"))]))
        story.append(widget)

    add("RNMP / ДОМАШНА ЗАДАЧА 1", small)
    add("GitHub активности<br/>во реално време", title)
    add("Producer, Java Flink апликација и dashboard", section)
    add("Кратка техничка документација · проект hw1-231035 · 23.11.2026", small)
    heading("Податочно множество и клуч")
    add(f"Датотека: <b>{escape(data['filename'])}</b>, обезбедена во материјалите од вежби. "
        "Не е вклучена во репозиториумот или ZIP архивата. Се чита директно како gzip JSON Lines.")
    table([["Својство", "Измерена вредност"], ["Настани / корисници", f"{data['events']:,} / {data['users']:,}"],
           ["Временски опсег (UTC)", f"{data['first_event_utc']} до {data['last_event_utc']}"],
           ["Kafka и Flink клуч", "actor.id: корисникот што го извршил настанот"],
           ["Event time", "created_at; timestamp во epoch milliseconds"]], [170, 337])
    heading("Архитектура")
    story.append(Architecture())
    add("Папката python се отвора во PyCharm, а java/pom.xml во IntelliJ како Maven проект. "
        "Python е основата од RNMP_homework1: оригинален consumer и Kafka Compose, со наш producer. Коренскиот Compose е само за Flink. "
        "Се користат Flink 2.3.0 и Java 17. Двата излеза се JSON пораки со user ID како Kafka клуч. "
        "Dashboard-от само набљудува; не ја заменува Flink обработката.")
    add("Kafka конектор: 5.0.0-2.2, проверен со овој проект на Flink 2.3.0. Apache сè уште нема објавено "
        "конектор специјално за 2.3; ова не е општа гаранција за компатибилност.", small)
    add("Основа: flink-demo (1).zip од аудиториски вежби. Common ги собира Kafka и watermark helpers; "
        "Steps 3, 5, 6 и 7 се применети за event time, агрегации, state/timers и сесии. "
        "Maven го следи и автоматскиот ide профил од вежбите.", small)
    add("SHA-256 на курсната датотека: " + data["sha256"][:32] + "<br/>" + data["sha256"][32:], small)

    story.append(PageBreak())
    add("ОБРАБОТКА НА ПОТОКОТ", small)
    add("Producer и двата модула", title)
    heading("Producer: репродукција со доцнење")
    add("Python producer-от ги вчитува и подредува настаните по created_at. Kafka клучот е UTF-8 actor.id. "
        "Оригиналниот JSON се задржува, а се додаваат key и timestamp; истиот event timestamp се поставува и во Kafka record.")
    add("Брзината е прилагодлива: --speed 60 значи 60 секунди од податоците за една реална секунда. "
        "Со seed 42 се избираат 10% од записите за доцнење од 120 data секунди. Се менува времето на испраќање, "
        "не timestamp-от. Во снименото извршување има 358 одложени записи и 357 глобално out-of-order пристигнувања. "
        "Producer-от чека Kafka acknowledgement; реалното траење може да биде подолго од идеалното.")
    heading("Аналитички модул: кориснички сесии")
    add("UserAnalytics користи keyBy(event.userId) и native EventTimeSessionWindows со <b>30-минутен inactivity gap</b>. "
        "Сесијата почнува со првиот настан, а крајот е последниот настан + 30 минути. Таа може да трае подолго од 30 минути. "
        "Настани за истиот корисник во различни repositories припаѓаат во истата историја.")
    add("Пример: 15:00 и 15:20 се една сесија [15:00, 15:50). Следен настан во 15:55 почнува нова сесија. "
        "Native Flink ги спојува и интервалите што се допираат: точен gap од 30 минути се спојува ако претходната сесија "
        "сè уште е отворена. Out-of-order настан може да поврзе две сесии; AggregateFunction.merge ги собира броевите "
        "и ги обединува множествата од repositories.")
    table([["JSON излез", "Содржина"], ["Идентитет и граници", "user_id, user_login, window_start, window_end, window_type=session, session_gap_minutes=30"],
           ["Агрегации", "event_count, unique_repositories, push / pull request / issue / star / fork / other counts"]], [170, 337])
    heading("Event-driven модул: неактивност во repository")
    add("RepositoryInactivityDetector е KeyedProcessFunction по корисник. MapState ги буферира настаните до watermark, "
        "а посебна MapState чува последна активност, deadline и број на настани за секое repository на тој корисник. "
        "Секој GitHub настан со repository ID се смета за активност; активност во B не го поместува deadline-от за A.")
    add("Секој настан поставува deadline на timestamp + 30 минути. Кога watermark ќе го достигне, тајмерот директно "
        "емитува USER_REPOSITORY_INACTIVE, без да чека корисникот повторно да придонесе. JSON содржи user ID, repo ID/име, "
        "последна активност и deadline. Настан точно на отворен deadline прво го продолжува рокот. Стар тајмер се "
        "брише ако не го дели друго repository или буфериран настан; по алармот состојбата за епизодата се отстранува.")

    story.append(PageBreak())
    add("EVENT TIME И МЕРЕЊА", small)
    add("Watermarks и offsets", title)
    heading("Watermark стратегија")
    add("Flink користи forBoundedOutOfOrderness(180 секунди), над доцнењето од 120 секунди. "
        "Сите овие траења се во event time, независно од speed. Сесијата емитува кога watermark-от ќе го достигне "
        "window_end - 1 ms. Kafka timestamp-от на резултатот е истата вредност.")
    add("Во live режим, запирање на producer-от не е крај на Kafka изворот. Ако нема понови timestamps, watermark-от "
        "не напредува и последните сесии и тајмери остануваат pending. При bounded replay, крајот испраќа MAX_WATERMARK. "
        "За Docker live job, stop --drain прави финално затворање. Обичен Stop/Cancel не го испразнува tail-от. "
        "Алармите затворени со финалниот watermark се означени со finalized_by_end_of_input=true; не претставуваат "
        "30 минути реално чекање. Idleness е 0: единствената партиција останува активна за набљудувањето.")
    heading("Earliest наспроти latest: исти влезни пораки")
    add("Earliest е стартуван откако податоците веќе се во Kafka. Latest е стартуван на празна тема, а producer-от "
        "дури откако клиентот потврдил почетен offset 0. Извршувањето користи свежи резултатски теми, една партиција, "
        f"Flink {report['flink_version']}, Java 17, брзина {report['speed']:g}×, "
        "30-минутен session gap и 30-минутен repository inactivity deadline.")
    table([["Мерење", "Earliest", "Latest"],
           ["Flink job со набљудување и drain (s)", f"{first['flink_job_seconds_including_observation_and_drain']:.3f}",
            f"{latest['flink_job_seconds_including_observation_and_drain']:.3f}"],
           ["Producer (s)", f"{first['producer']['wall_seconds']:.3f}" if first['producer'] else "Претходно вчитан", f"{latest['producer']['wall_seconds']:.3f}"],
           ["Сесии пред / по drain", f"{first['before_drain']['analytics']['count']} / {first['after_drain']['analytics']['count']}",
            f"{latest['before_drain']['analytics']['count']} / {latest['after_drain']['analytics']['count']}"],
           ["Известувања пред / по drain", f"{first['before_drain']['events']['count']} / {first['after_drain']['events']['count']}",
            f"{latest['before_drain']['events']['count']} / {latest['after_drain']['events']['count']}"],
           ["Буферирани настани пред drain", str(first['metrics_before_drain']['events_received'] - first['metrics_before_drain']['events_finalized']),
            str(latest['metrics_before_drain']['events_received'] - latest['metrics_before_drain']['events_finalized'])]], [260, 123, 124])
    add(f"<b>По drain резултатите се исти:</b> {first['after_drain']['analytics']['count']:,} сесии од сите "
        f"{report['input_records']:,} настани и {first['after_drain']['events']['count']:,} repository inactivity известувања. "
        "Споредени се JSON вредности, Kafka клучеви и timestamps, независно од редоследот на излезите. "
        "Влезните пораки и нивниот редослед се потврдени со SHA-256.")
    add("Разликата во траењето е очекувана: latest го вклучува чекањето на producer-от. Мерењата вклучуваат startup, "
        "намерно набљудување и drain; тие не се чист benchmark на операторите. Raw мерењата се во "
        "reports/offset-comparison.json, а читливиот извештај во reports/offset-comparison.md.", small)

    story.append(PageBreak())
    add("КОРИСТЕЊЕ И ПРЕДАВАЊЕ", small)
    add("Dashboard и репродукција", title)
    heading("Што се гледа на dashboard-от")
    add("На http://127.0.0.1:8501 се прикажуваат Kafka input записи, dashboard-consumed записи, offset/lag, "
        "набљудувана стапка на пристигнување, event types, out-of-order бројач, корисничка временска линија, "
        "потврдени сесии и repository inactivity известувања. Табелата го покажува repository-то и дали алармот е "
        "затворен од нормален watermark или од end of input. User filter, JSON envelope, Pause view и Export view овозможуваат "
        "проверка на конкретен корисник. Connection settings ги избира темите и offset-от само за dashboard-от.")
    add("<b>Полни интервали:</b> вистински резултати од Flink output topic. <b>Шрафирани интервали:</b> preview од "
        "задржаниот влез со 30-минутен gap; не се Flink state или гарантиран резултат. Приказот задржува до 20,000 "
        "input summaries, 10,000 сесии и 2,000 известувања. Големите payload bodies не се чуваат во dashboard-от. "
        "Стапката вклучува историски catch-up и не е директно мерење на producer-от. Flink counters и watermark се "
        "прикажуваат само кога се достапни од соодветен REST job.")
    heading("Стартување од IDE")
    add("1. Стартувај Docker Desktop и Kafka од python/docker-compose.yml. Во PyCharm отвори python и избери .venv/Scripts/python.exe. "
        "Во IntelliJ отвори java/pom.xml како Maven проект и избери Java 17.")
    add("2. Во PyCharm избери <b>Dashboard - Stream observatory</b>. Во IntelliJ стартувај "
        "<b>GitHub - Live (both modules)</b>. Потоа во PyCharm стартувај <b>Producer - IDE live</b>. "
        "Конфигурациите ги делат github-users-repo1, github-user-sessions-repo1 и github-user-alerts-repo1.")
    add("За веќе наполнет github-users-run3 користи <b>GitHub - Replay run3 (both modules)</b> и избери го истиот "
        "влезен topic во dashboard-от. Секој чист нов replay користи свежи влезни и излезни теми.")
    add("docker compose -f python/docker-compose.yml up -d --wait<br/>"
        "./run.sh consume_messages.py github-user-sessions-repo1", code)
    heading("Пакет за Moodle")
    add("README содржи точни PowerShell команди за setup, producer, Java Flink апликацијата, dashboard и "
        "earliest/latest споредбата. Java проектот се пакува со Maven package; двата модула се избираат преку --job. "
        "Измерените резултати од споредбата се зачувани како JSON и Markdown извештаи.")
    add(escape(r".\python\.venv\Scripts\python.exe python/package_submission.py"), code)
    add("Архивата output/submission/hw1-231035.zip содржи producer, dashboard source, Java Maven проект, "
        "README, PDF и агрегирани мерења. Не содржи dataset, виртуелни околини, .idea, database или build target. "
        "На Moodle се предава архивата или линк до сопствен Git репозиториум со пристап за асистентот.")
    heading("Референци")
    add('<link href="https://github.com/stefanandonov/RNMP_homework1" color="#4e65d9">Код од вежби: RNMP_homework1</link><br/>'
        '<link href="https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/operators/windows/" color="#4e65d9">Apache Flink 2.3: event-time session windows</link><br/>'
        '<link href="https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/cli/" color="#4e65d9">Apache Flink CLI: stop --drain</link><br/>'
        '<link href="https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/connectors/datastream/kafka/" color="#4e65d9">Apache Flink 2.3: Kafka connector availability</link>', small)

    def decorate(canvas, document):
        width, height = A4
        canvas.setStrokeColor(colors.HexColor("#e1e7f1"))
        canvas.line(44, 40, width - 44, 40)
        canvas.setFillColor(MUTED)
        canvas.setFont("Body", 8)
        canvas.drawString(44, 25, "hw1-231035 · GitHub stream observatory")
        canvas.drawRightString(width - 44, 25, f"{document.page:02d}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    document = SimpleDocTemplate(str(args.output), pagesize=A4, rightMargin=44, leftMargin=44,
                                 topMargin=39, bottomMargin=56, title="GitHub Stream Observatory - RNMP документација",
                                 author="hw1-231035")
    document.build(story, onFirstPage=decorate, onLaterPages=decorate)
    print(args.output)


if __name__ == "__main__":
    main()
