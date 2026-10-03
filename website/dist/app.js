"use strict";

// This is an explanatory simulation, not a client for the Jev API.
const scenarios = {
  rules: {
    input: '“action: cancel, order_id: 1042”',
    inputType: 'explicit action',
    result: 'order.cancelled',
    rules: 'rule matched', cache: 'not needed', jev: 'not needed',
    explanation: 'The explicit-cancel rule wins. The cache and Jev are never called.',
    skipped: ['cache', 'jev']
  },
  cache: {
    input: '“The property at 1 Main St was sold for $500,000.”',
    inputType: 'seen before',
    result: 'property.sold',
    rules: 'no match', cache: 'decision found', jev: 'not needed',
    explanation: 'No matching rule. Reuse a cached decision for this state and candidate set.',
    skipped: ['jev']
  },
  jev: {
    input: '“The property at 1 Main St was sold for $500,000.”',
    inputType: 'unstructured input',
    result: 'property.sold',
    rules: 'no match', cache: 'cache miss', jev: 'decision selected',
    explanation: 'No matching rule. No cached decision. Jev selects from your candidates.',
    skipped: []
  }
};

const scenarioButtons = [...document.querySelectorAll('[data-scenario]')];
function selectScenario(name) {
  const scenario = scenarios[name];
  scenarioButtons.forEach(button => button.setAttribute('aria-pressed', String(button.dataset.scenario === name)));
  document.querySelector('.pipeline').dataset.active = name;
  document.getElementById('event-input').textContent = scenario.input;
  document.querySelector('.input-tag').textContent = scenario.inputType;
  document.getElementById('selected-event').textContent = scenario.result;
  document.getElementById('result-explanation').textContent = scenario.explanation;
  ['rules', 'cache', 'jev'].forEach(stage => {
    document.getElementById(stage + '-status').textContent = scenario[stage];
    const node = document.getElementById('node-' + stage);
    node.classList.toggle('selected', stage === name);
    node.classList.toggle('skipped', scenario.skipped.includes(stage));
  });
}
scenarioButtons.forEach(button => button.addEventListener('click', () => selectScenario(button.dataset.scenario)));

const directExample = document.getElementById('example-code').textContent;
const intelligentExample = `package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    bus "github.com/Protocol-Lattice/GoEventBus"
)

func main() {
    ctx := context.Background()
    dispatcher := bus.Dispatcher{}
    for _, name := range []string{"order.cancelled", "property.sold"} {
        dispatcher.Register(name, func(_ context.Context, ev bus.Event) (bus.Result, error) {
            fmt.Println(ev.Projection, ev.Data)
            return bus.Result{}, nil
        })
    }
    store := bus.NewEventStore(&dispatcher, 1024, bus.Block)
    defer store.Close(ctx)

    selector := &bus.RuleCacheSelector{
        Rules: []bus.EventRule{{
            Name: "explicit-cancel", Choice: "cancel",
            Match: func(_ context.Context, state any, _ []bus.EventCandidate) bool {
                input, ok := state.(map[string]any)
                return ok && input["action"] == "cancel"
            },
        }},
        Cache: bus.NewMemoryDecisionCache(5 * time.Minute),
        Fallback: &bus.JevSelector{
            APIKey: os.Getenv("OPENROUTER_API_KEY"),
        },
    }

    candidates := []bus.EventCandidate{
        {Key: "cancel", Projection: "order.cancelled", Description: "Cancel an order"},
        {Key: "sale", Projection: "property.sold", Description: "A property was sold"},
    }
    state := map[string]any{
        "message": "The property at 1 Main St was sold for $500,000",
    }
    decision, err := store.DecideAndSubscribe(
        ctx, selector, state,
        bus.Event{ID: "evt-1", Data: state}, candidates,
    )
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Selected:", decision.Choice)
    store.Publish()
}`;

const examples = { direct: directExample, intelligent: intelligentExample };
let activeExample = 'direct';
const exampleTabs = [...document.querySelectorAll('[data-example]')];
const escapeHTML = value => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
function highlightLine(line) {
  const tokens = /(\/\/.*$|"(?:[^"\\]|\\.)*"|\b(?:package|import|func|return|defer|if|for|range|nil|any|string|bool)\b|\b\d+\b)/g;
  let result = '';
  let previousEnd = 0;
  for (const match of line.matchAll(tokens)) {
    result += escapeHTML(line.slice(previousEnd, match.index));
    const token = match[0];
    const type = token.startsWith('//') ? 'comment' : token.startsWith('"') ? 'string' : /^\d+$/.test(token) ? 'number' : 'keyword';
    result += '<span class="syntax-' + type + '">' + escapeHTML(token) + '</span>';
    previousEnd = match.index + token.length;
  }
  return result + escapeHTML(line.slice(previousEnd));
}
function renderExample(name) {
  activeExample = name;
  exampleTabs.forEach(tab => {
    const selected = tab.dataset.example === name;
    tab.setAttribute('aria-selected', String(selected));
    tab.tabIndex = selected ? 0 : -1;
  });
  const code = document.getElementById('example-code');
  code.innerHTML = examples[name].split('\n').map((line, index) => '<span class="code-line"><span class="line-number" aria-hidden="true">' + (index + 1) + '</span>' + highlightLine(line) + '</span>').join('');
  const panel = document.getElementById('code-panel');
  panel.setAttribute('aria-labelledby', 'tab-' + name);
  panel.scrollTop = 0;
  panel.scrollLeft = 0;
  document.getElementById('code-description').textContent = name === 'direct' ? 'SYNCHRONOUS DISPATCH' : 'OPTIONAL DECISION LAYER';
  document.getElementById('code-status').textContent = name === 'direct' ? 'READY TO RUN' : 'REQUIRES OPENROUTER_API_KEY';
  document.getElementById('copy-code').textContent = 'Copy code';
}
exampleTabs.forEach((tab, index) => {
  tab.addEventListener('click', () => renderExample(tab.dataset.example));
  tab.addEventListener('keydown', event => {
    let next;
    if (event.key === 'ArrowRight') next = (index + 1) % exampleTabs.length;
    else if (event.key === 'ArrowLeft') next = (index - 1 + exampleTabs.length) % exampleTabs.length;
    else if (event.key === 'Home') next = 0;
    else if (event.key === 'End') next = exampleTabs.length - 1;
    else return;
    event.preventDefault();
    renderExample(exampleTabs[next].dataset.example);
    exampleTabs[next].focus();
  });
});

async function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch { /* Use the selection-based fallback below. */ }
  }
  const activeElement = document.activeElement;
  const field = document.createElement('textarea');
  field.value = text;
  field.style.cssText = 'position:fixed;top:0;left:-9999px;';
  field.setAttribute('readonly', '');
  document.body.append(field);
  field.select();
  let copied = false;
  try { copied = document.execCommand('copy'); } catch { copied = false; }
  field.remove();
  activeElement?.focus();
  return copied;
}

document.getElementById('copy-install').addEventListener('click', async () => {
  const copied = await copyText(document.getElementById('install-command').textContent);
  document.getElementById('install-status').textContent = copied ? 'Installation command copied.' : 'Select the command above to copy it.';
});
document.getElementById('copy-code').addEventListener('click', async () => {
  const name = activeExample;
  const copied = await copyText(examples[name]);
  if (name !== activeExample) return;
  document.getElementById('copy-code').textContent = copied ? 'Copied!' : 'Select to copy';
  document.getElementById('code-status').textContent = copied ? 'CODE COPIED' : 'SELECT THE CODE TO COPY';
});

renderExample('direct');
