"use strict";
const $ = id => document.getElementById(id);
const fmt = value => Number(value || 0).toLocaleString("en-US");
const clock = value => value == null ? "—" : new Date(value).toISOString().slice(11, 19);
const isoMs = value => Date.parse(value.endsWith("Z") || /[+-]\d\d:\d\d$/.test(value) ? value : value + "Z");
let state = null, tab = "analytics", paused = false, polling = false, pendingFilter = null;
const SVG = "http://www.w3.org/2000/svg";
function svgNode(tag, attrs = {}, text) { const node = document.createElementNS(SVG, tag); for (const [key, val] of Object.entries(attrs)) node.setAttribute(key, val); if (text != null) node.textContent = text; return node; }
function node(tag, text, className) { const el = document.createElement(tag); if (text != null) el.textContent = text; if (className) el.className = className; return el; }

function drawRate() {
  const svg = $("rate-chart"); svg.replaceChildren();
  const values = state.rates, max = Math.max(1, ...values.map(point => point.rate));
  const defs = svgNode("defs"), gradient = svgNode("linearGradient", {id:"rate-gradient", x1:0,y1:0,x2:0,y2:1});
  gradient.append(svgNode("stop", {offset:"0%", "stop-color":"#6175f0", "stop-opacity":.17}), svgNode("stop", {offset:"100%", "stop-color":"#6175f0", "stop-opacity":.01})); defs.append(gradient); svg.append(defs);
  for (let i = 0; i < 4; i++) { const y = 20 + i * 36; svg.append(svgNode("line", {x1:38,y1:y,x2:685,y2:y,stroke:"#edf0f7","stroke-dasharray":"3 4"}), svgNode("text", {x:28,y:y+4,fill:"#a0aabd","font-size":10,"text-anchor":"end"}, Math.round(max*(1-i/3)))); }
  const points = values.map((point, i) => [38 + i / Math.max(1,values.length-1) * 647,128-point.rate/max*108]);
  if (points.length) { const line = points.map(point=>point.join(",")).join(" "); svg.append(svgNode("polygon", {points:`38,128 ${line} ${points.at(-1)[0]},128`,fill:"url(#rate-gradient)"}), svgNode("polyline",{points:line,fill:"none",stroke:"#6479ee","stroke-width":2,"stroke-linejoin":"round"})); }
  if (values.length) { svg.append(svgNode("text",{x:38,y:153,fill:"#9ba7bc","font-size":10},clock(values[0].time)), svgNode("text",{x:685,y:153,fill:"#9ba7bc","font-size":10,"text-anchor":"end"},clock(values.at(-1).time))); }
  $("current-rate").textContent = (values.at(-1)?.rate || 0).toFixed(1) + " / sec";
}

function drawMix() {
  const root = $("event-mix"); root.replaceChildren();
  const colors = ["#536aec","#91a4fa","#60b8ba","#b6a4e4","#94c6e1"];
  const values = state.event_types.slice(0,5), max = Math.max(1,...values.map(row=>row[1]));
  if (!values.length) root.append(node("div","Event types will appear here.","empty-small"));
  values.forEach(([name,count],i)=>{ const row=node("div",null,"mix-row"), track=node("div",null,"mix-track"),fill=node("div",null,"mix-fill"); fill.style.width=`${count/max*100}%`;fill.style.background=colors[i];track.append(fill);row.append(node("span",name.replace(/Event$/,""),"mix-label"),track,node("span",fmt(count),"mix-count"));root.append(row); });
}

function drawTimeline() {
  const svg = $("timeline"), rows = state.previews; svg.replaceChildren();
  $("timeline-empty").hidden = rows.length > 0; svg.hidden = !rows.length; if (!rows.length) return;
  const users = [...new Set(rows.map(row=>row.key))].slice(0,8);
  const min = Math.min(...rows.map(row=>row.start)), max = Math.max(...rows.map(row=>row.end));
  const x = timestamp => 145 + (timestamp-min)/Math.max(1,max-min)*825, height=users.length*30+42;
  svg.setAttribute("viewBox",`0 0 1000 ${height}`);
  const defs=svgNode("defs"),pattern=svgNode("pattern",{id:"stripes",width:6,height:6,patternUnits:"userSpaceOnUse",patternTransform:"rotate(45)"});pattern.append(svgNode("rect",{width:6,height:6,fill:"#ecf0ff"}),svgNode("rect",{width:2,height:6,fill:"#bbc8ff"}));defs.append(pattern);svg.append(defs);
  for(let i=0;i<6;i++){const time=min+(max-min)*i/5;svg.append(svgNode("line",{x1:x(time),x2:x(time),y1:24,y2:height-12,stroke:"#edf0f7","stroke-dasharray":"3 4"}),svgNode("text",{x:x(time),y:13,fill:"#94a0b5","font-size":10,"text-anchor":"middle"},clock(time)));}
  users.forEach((key,index)=>{const y=35+index*30, sessions=rows.filter(row=>row.key===key);const label=svgNode("text",{x:0,y:y+9,fill:"#71809a","font-size":11},sessions[0].login.slice(0,19));label.style.cursor="pointer";label.addEventListener("click",()=>{$("user-filter").value=key;refresh();});svg.append(label);
    sessions.forEach(session=>{const rect=svgNode("rect",{x:x(session.start),y,width:Math.max(3,x(session.end)-x(session.start)),height:13,rx:4,fill:session.confirmed?"#6075ed":"url(#stripes)"});rect.append(svgNode("title",{},`${session.login}: ${session.event_count} events; ${clock(session.start)}–${clock(session.end)} UTC; ${session.confirmed?"Flink confirmed":"input preview"}`));svg.append(rect);session.timestamps.forEach(time=>svg.append(svgNode("circle",{cx:x(time),cy:y+6.5,r:2.2,fill:session.confirmed?"#f6f8ff":"#7288d9"})));});
  });
  // The reported Flink watermark is optional; never substitute a wall-clock estimate.
  const watermark=state.flink.metrics?.currentInputWatermark;
  if(watermark>min && watermark<max){svg.append(svgNode("line",{x1:x(watermark),x2:x(watermark),y1:20,y2:height-10,stroke:"#ddab51","stroke-dasharray":"4 3"}),svgNode("title",{},`Flink watermark: ${clock(watermark)} UTC`));}
}

function inspect(title,value){$("inspect-title").textContent=title;$("inspect-json").textContent=JSON.stringify(value,null,2);$("inspect-dialog").showModal();}
function renderTable(){
  const columns = tab==="analytics" ? ["USER","SESSION START (UTC)","SESSION END (UTC)","EVENTS","REPOSITORIES","STATUS"] : tab==="input" ? ["USER","TYPE","EVENT TIME (UTC)","REPOSITORY","PARTITION / OFFSET"] : ["USER","REPOSITORY","LAST ACTIVITY (UTC)","INACTIVE AT (UTC)","FINALIZED BY","OFFSET"];
  const header=node("tr");columns.forEach(title=>header.append(node("th",title)));$("table-head").replaceChildren(header);$("table-body").replaceChildren();
  const rows = [...state.rows[tab]].reverse();
  rows.forEach(row=>{const value=row.value,tr=node("tr");let cells;
    if(tab==="analytics")cells=[value.user_login||row.key,clock(isoMs(value.window_start)),clock(isoMs(value.window_end)),fmt(value.event_count),fmt(value.unique_repositories),"Confirmed"];
    else if(tab==="input")cells=[value.actor?.login||row.key,value.type||"Unknown",clock(row.timestamp),value.repo?.name||"—",`${row.partition} / ${row.offset}`];
    else cells=[value.user_login||row.key,value.repository_name||value.repository_id||"—",clock(value.last_activity_timestamp_ms),clock(value.inactive_at_ms),value.finalized_by_end_of_input?"End of input":"Watermark",String(row.offset)];
    cells.forEach((text,index)=>{const td=node("td");if((tab==="analytics" && index===5)||(tab==="input" && index===1))td.append(node("span",text,tab==="analytics"?"badge confirmed-badge":"badge"));else td.textContent=text;tr.append(td);});
    tr.tabIndex=0;tr.setAttribute("aria-label",`Inspect Kafka record for user ${row.key}`);tr.addEventListener("click",()=>inspect(tab==="input"?"Input Kafka envelope (event summary)":"Kafka result envelope",row));tr.addEventListener("keydown",event=>{if(event.key==="Enter")tr.click();});$("table-body").append(tr);
  });
  $("table-empty").hidden=rows.length>0;
  if(!rows.length){$("table-empty").replaceChildren(node("div",tab==="analytics"?"Waiting for completed sessions.":tab==="input"?"No input events observed yet.":"No repository inactivity alerts observed yet."),node("small",tab==="analytics"?"A session closes when the watermark passes the last event plus 30 minutes. Final live sessions can remain pending after the producer stops.":"Check the connection topics, then start the producer and Java application."));}
  $("table-count").textContent=`${fmt(rows.length)} records shown · ${$("user-filter").value ? "filtered view" : "recent retained records"}`;
}

function render(){
  $("connection-status").textContent=state.status==="connected"?"Kafka connected":state.status==="connecting"?"Connecting":"Kafka disconnected";$("connection-status").className="status "+state.status;
  $("error-banner").hidden=!state.error;$("error-banner").textContent=state.error||"";
  const input=state.offsets[state.connection.input_topic];
  $("input-total").textContent=fmt(input?.retained_records);$("consumed-total").textContent=fmt(state.counts.input);$("lag-total").textContent=fmt(input?.lag);$("session-total").textContent=fmt(state.counts.analytics);$("alert-total").textContent=fmt(state.counts.events);$("disorder-total").textContent=fmt(state.out_of_order);$("session-tab-count").textContent=fmt(state.counts.analytics);
  $("input-topic").textContent=state.connection.input_topic;$("input-topic").title=state.connection.input_topic;
  $("producer-state").textContent=state.max_event_timestamp?`Latest event: ${clock(state.max_event_timestamp)} UTC`:"Waiting for Kafka input";
  $("flink-state").textContent=state.flink.available?`Job ${state.flink.state.toLowerCase()}${state.flink.metrics?.events_received!=null?" · "+fmt(state.flink.metrics.events_received)+" received":""}`:state.counts.analytics?"Session output observed in Kafka":"Awaiting results · REST optional";
  $("flink-state").title=state.flink.message||state.flink.job_id||"";
  $("last-updated").textContent="Updated "+clock(state.server_time)+" UTC";$("footer-offset").textContent=`Dashboard source: ${state.connection.start_offset} · no committed consumer offsets`;
  drawRate();drawMix();drawTimeline();renderTable();
}

async function refresh(){if(paused||polling)return;polling=true;try{const response=await fetch("/api/state?user="+encodeURIComponent($("user-filter").value),{cache:"no-store"});if(!response.ok)throw new Error("Dashboard server returned "+response.status);state=await response.json();render();}catch(error){$("connection-status").textContent="Server unavailable";$("connection-status").className="status disconnected";$("error-banner").hidden=false;$("error-banner").textContent=error.message;}finally{polling=false;}}
document.querySelectorAll("[data-tab]").forEach(button=>button.addEventListener("click",()=>{tab=button.dataset.tab;document.querySelectorAll("[data-tab]").forEach(item=>item.classList.toggle("active",item===button));if(state)renderTable();}));
document.querySelectorAll("[data-view]").forEach(button=>button.addEventListener("click",()=>{document.querySelectorAll("[data-view]").forEach(item=>item.classList.toggle("selected",item===button));const view=button.dataset.view;document.querySelectorAll(".overview-section").forEach(item=>item.hidden=view!=="overview");if(view!=="overview"){tab=view==="sessions"?"analytics":view;document.querySelectorAll("[data-tab]").forEach(item=>item.classList.toggle("active",item.dataset.tab===tab));if(state)renderTable();}else{document.querySelector(".timeline-panel").scrollIntoView({behavior:"smooth",block:"nearest"});}}));
$("user-filter").addEventListener("input",()=>{clearTimeout(pendingFilter);pendingFilter=setTimeout(refresh,250);});
$("pause-button").addEventListener("click",()=>{paused=!paused;$("pause-button").textContent=paused?"Resume view":"Pause view";if(!paused)refresh();});
$("export-button").addEventListener("click",()=>{if(!state)return;const content={exported_at:new Date().toISOString(),connection:state.connection,filter:$("user-filter").value,rows:state.rows[tab]};const url=URL.createObjectURL(new Blob([JSON.stringify(content,null,2)],{type:"application/json"}));const link=node("a");link.href=url;link.download=`github-${tab}-view.json`;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);});
$("settings-button").addEventListener("click",()=>{if(!state)return;const form=$("settings-form");for(const[key,value]of Object.entries(state.connection))form.elements.namedItem(key).value=value;$("settings-error").textContent="";$("settings-dialog").showModal();});
document.querySelectorAll(".close-dialog").forEach(button=>button.addEventListener("click",()=>button.closest("dialog").close()));
$("settings-form").addEventListener("submit",async event=>{event.preventDefault();const values=Object.fromEntries(new FormData(event.target));try{const response=await fetch("/api/connect",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(values)});const result=await response.json();if(!response.ok)throw new Error(result.error);paused=false;$("pause-button").textContent="Pause view";$("user-filter").value="";$("settings-dialog").close();await refresh();}catch(error){$("settings-error").textContent=error.message;}});
$("comparison-button").addEventListener("click",async()=>{try{const response=await fetch("/api/report");inspect("Recorded earliest / latest experiment",await response.json());}catch(error){inspect("Comparison unavailable",{error:error.message});}});
refresh();setInterval(refresh,1000);
