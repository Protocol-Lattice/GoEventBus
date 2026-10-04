"use strict";

(() => {
  const input = document.getElementById('doc-search-input');
  const search = document.querySelector('.doc-search');
  const panel = document.getElementById('doc-search-panel');
  const results = document.getElementById('doc-search-results');
  const status = document.getElementById('doc-search-status');
  const clear = document.getElementById('doc-search-clear');
  const empty = document.getElementById('doc-search-empty');
  const menu = document.getElementById('doc-menu');
  const mobile = window.matchMedia('(max-width: 800px)');
  const normalize = text => text.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const compact = text => text.replace(/\s+/g, ' ').trim();

  // Index the rendered README so search cannot drift from the visible docs.
  // Capture plain text before adding syntax highlighting or copy feedback.
  const entries = [...document.querySelectorAll('[data-search-section]')].map(section => {
    const content = section.cloneNode(true);
    content.querySelectorAll('.heading-anchor, .doc-code-toolbar, .doc-section-label').forEach(node => node.remove());
    const text = compact(content.textContent);
    return {
      id: section.id,
      title: section.dataset.title,
      category: section.dataset.category,
      text,
      titleKey: normalize(section.dataset.title),
      textKey: normalize(text)
    };
  });

  let matches = [];
  let selected = -1;

  function highlight(text, words) {
    const fragment = document.createDocumentFragment();
    const normalized = normalize(text);
    const ranges = [];
    for (const word of words) {
      let start = normalized.indexOf(word);
      while (start !== -1) {
        ranges.push([start, start + word.length]);
        start = normalized.indexOf(word, start + word.length);
      }
    }
    ranges.sort((a, b) => a[0] - b[0]);
    const merged = [];
    for (const range of ranges) {
      const last = merged[merged.length - 1];
      if (last && range[0] <= last[1]) last[1] = Math.max(last[1], range[1]);
      else merged.push(range);
    }
    let offset = 0;
    for (const [start, end] of merged) {
      fragment.append(text.slice(offset, start));
      const mark = document.createElement('mark');
      mark.textContent = text.slice(start, end);
      fragment.append(mark);
      offset = end;
    }
    fragment.append(text.slice(offset));
    return fragment;
  }

  function snippet(entry, words) {
    const positions = words.map(word => entry.textKey.indexOf(word)).filter(index => index >= 0);
    const position = positions.length ? Math.min(...positions) : 0;
    const start = Math.max(0, position - 45);
    const end = Math.min(entry.text.length, start + 175);
    return (start ? '…' : '') + entry.text.slice(start, end) + (end < entry.text.length ? '…' : '');
  }

  function closeResults() {
    panel.hidden = true;
    input.setAttribute('aria-expanded', 'false');
    input.removeAttribute('aria-activedescendant');
    selected = -1;
  }

  function selectResult(index) {
    if (!matches.length) return;
    selected = (index + matches.length) % matches.length;
    [...results.children].forEach((option, i) => option.setAttribute('aria-selected', String(i === selected)));
    const option = results.children[selected];
    input.setAttribute('aria-activedescendant', option.id);
    // Scroll only the results panel, never the document behind it.
    if (option.offsetTop < panel.scrollTop) panel.scrollTop = option.offsetTop;
    else if (option.offsetTop + option.offsetHeight > panel.scrollTop + panel.clientHeight) {
      panel.scrollTop = option.offsetTop + option.offsetHeight - panel.clientHeight;
    }
  }

  function focusSection(id) {
    const section = document.getElementById(id);
    if (!section) return;
    if (mobile.matches) menu.open = false;
    closeResults();
    const heading = section.querySelector('h1, h2, h3');
    heading?.focus({ preventScroll: true });
    section.scrollIntoView({ block: 'start', behavior: 'instant' });
    updateActiveSection();
  }

  function openResult(entry) {
    window.location.hash = entry.id;
    focusSection(entry.id);
  }

  function searchDocs() {
    const query = normalize(compact(input.value));
    clear.hidden = input.value.length === 0;
    input.removeAttribute('aria-activedescendant');
    selected = -1;
    results.replaceChildren();
    if (!query) {
      matches = [];
      closeResults();
      return;
    }
    const words = [...new Set(query.split(' '))];
    const ranked = entries.map(entry => {
      if (!words.every(word => entry.titleKey.includes(word) || entry.textKey.includes(word))) return null;
      let score = entry.titleKey === query ? 100 : entry.titleKey.includes(query) ? 60 : 0;
      for (const word of words) {
        if (entry.titleKey.includes(word)) score += 15;
      }
      return { entry, score };
    }).filter(Boolean).sort((a, b) => b.score - a.score || a.entry.text.length - b.entry.text.length);
    matches = ranked.slice(0, 10).map(match => match.entry);
    status.textContent = ranked.length === 0
      ? `No results for “${compact(input.value)}”`
      : `${ranked.length} ${ranked.length === 1 ? 'result' : 'results'}${ranked.length > matches.length ? ' · showing the first 10' : ''}`;
    empty.hidden = matches.length > 0;
    for (const [index, entry] of matches.entries()) {
      const option = document.createElement('a');
      option.id = `doc-result-${index}`;
      option.className = 'doc-search-result';
      option.href = `#${entry.id}`;
      option.tabIndex = -1;
      option.setAttribute('role', 'option');
      option.setAttribute('aria-selected', 'false');
      const category = document.createElement('span');
      category.className = 'result-category';
      category.textContent = entry.category;
      const title = document.createElement('strong');
      title.append(highlight(entry.title, words));
      const excerpt = document.createElement('span');
      excerpt.className = 'result-snippet';
      excerpt.append(highlight(snippet(entry, words), words));
      option.append(category, title, excerpt);
      option.addEventListener('click', event => {
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        event.preventDefault();
        openResult(entry);
      });
      results.append(option);
    }
    panel.hidden = false;
    panel.scrollTop = 0;
    input.setAttribute('aria-expanded', 'true');
  }

  input.disabled = false;
  input.addEventListener('input', searchDocs);
  input.addEventListener('search', searchDocs);
  input.addEventListener('focus', () => { if (input.value.trim()) searchDocs(); });
  input.addEventListener('keydown', event => {
    if (event.isComposing) return;
    if (event.key === 'Escape') {
      event.preventDefault();
      if (panel.hidden) {
        input.value = '';
        searchDocs();
      } else closeResults();
    } else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      if (panel.hidden) searchDocs();
      selectResult(selected < 0 ? (event.key === 'ArrowDown' ? 0 : matches.length - 1) : selected + (event.key === 'ArrowDown' ? 1 : -1));
    } else if (event.key === 'Enter' && !panel.hidden && matches.length) {
      event.preventDefault();
      openResult(matches[Math.max(selected, 0)]);
    }
  });
  clear.addEventListener('click', () => {
    input.value = '';
    searchDocs();
    input.focus();
  });
  document.addEventListener('pointerdown', event => {
    if (!search.contains(event.target)) closeResults();
  });
  search.addEventListener('focusout', event => {
    if (!search.contains(event.relatedTarget)) closeResults();
  });
  document.addEventListener('keydown', event => {
    const editable = event.target.closest('input, textarea, select, [contenteditable]:not([contenteditable="false"])');
    const command = (event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k';
    const slash = event.key === '/' && !editable && !event.ctrlKey && !event.metaKey && !event.altKey;
    if (command || slash) {
      event.preventDefault();
      input.focus();
      input.select();
    }
  });
  document.getElementById('doc-search-shortcut').textContent = /Mac|iPhone|iPad/.test(navigator.platform) ? '⌘ K' : 'Ctrl K';

  const navLinks = [...menu.querySelectorAll('a[href^="#"]')];
  const chapters = [...document.querySelectorAll('[data-doc-section]')];
  const toolbar = document.querySelector('.docs-toolbar');
  let stickyHeight = 0;
  let scrollQueued = false;

  function updateActiveSection() {
    let active = chapters[0];
    for (const chapter of chapters) {
      if (chapter.getBoundingClientRect().top <= stickyHeight + 90) active = chapter;
      else break;
    }
    navLinks.forEach(link => {
      if (link.hash === `#${active.id}`) link.setAttribute('aria-current', 'location');
      else link.removeAttribute('aria-current');
    });
  }

  function updateStickyHeight() {
    const headerHeight = document.querySelector('.docs-header').offsetHeight;
    stickyHeight = toolbar.offsetHeight + (mobile.matches ? 0 : headerHeight);
    document.body.style.setProperty('--docs-header-height', `${headerHeight}px`);
    document.body.style.setProperty('--docs-sticky-height', `${stickyHeight}px`);
    updateActiveSection();
  }
  function updateMenu() {
    menu.open = !mobile.matches;
    updateStickyHeight();
  }
  menu.addEventListener('click', event => {
    const link = event.target.closest('a[href^="#"]');
    if (!link || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    window.location.hash = link.hash;
    focusSection(link.hash.slice(1));
  });
  window.addEventListener('scroll', () => {
    if (scrollQueued) return;
    scrollQueued = true;
    requestAnimationFrame(() => { updateActiveSection(); scrollQueued = false; });
  }, { passive: true });
  window.addEventListener('hashchange', () => {
    try { focusSection(decodeURIComponent(window.location.hash.slice(1))); } catch { /* Ignore malformed external fragments. */ }
  });
  mobile.addEventListener('change', updateMenu);
  window.addEventListener('resize', updateStickyHeight);
  if ('ResizeObserver' in window) {
    const observer = new ResizeObserver(updateStickyHeight);
    observer.observe(toolbar);
    observer.observe(document.querySelector('.docs-header'));
  }
  updateMenu();

  const escapeHTML = text => text.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
  function highlightCode(text) {
    return text.split('\n').map(line => {
      const tokens = /(\/\/.*$|"(?:[^"\\]|\\.)*"|\b(?:package|import|func|return|defer|if|for|range|nil|any|string|bool|type|struct|interface|map|var|go|int|error|float64|uint64)\b|\b\d+\b)/g;
      let result = '';
      let offset = 0;
      for (const match of line.matchAll(tokens)) {
        result += escapeHTML(line.slice(offset, match.index));
        const token = match[0];
        const type = token.startsWith('//') ? 'comment' : token.startsWith('"') ? 'string' : /^\d+$/.test(token) ? 'number' : 'keyword';
        result += `<span class="syntax-${type}">${escapeHTML(token)}</span>`;
        offset = match.index + token.length;
      }
      return result + escapeHTML(line.slice(offset));
    }).join('\n');
  }

  async function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) {
      try { await navigator.clipboard.writeText(text); return true; } catch { /* Fall back to a selected text field. */ }
    }
    const active = document.activeElement;
    const field = document.createElement('textarea');
    field.value = text;
    field.readOnly = true;
    field.style.cssText = 'position:fixed;top:0;left:-9999px';
    document.body.append(field);
    field.select();
    let copied = false;
    try { copied = document.execCommand('copy'); } catch { /* The user can still select the code. */ }
    field.remove();
    active?.focus({ preventScroll: true });
    return copied;
  }
  document.querySelectorAll('.doc-code').forEach(block => {
    const code = block.querySelector('code');
    const button = block.querySelector('.doc-copy');
    const text = code.textContent;
    if (code.dataset.language === 'go') code.innerHTML = highlightCode(text);
    button.hidden = false;
    let reset;
    button.addEventListener('click', async () => {
      button.disabled = true;
      const copied = await copyText(text);
      button.disabled = false;
      button.textContent = copied ? 'Copied!' : 'Select to copy';
      document.getElementById('doc-copy-status').textContent = copied ? 'Code copied to clipboard.' : 'Copy was unavailable. Select the code to copy it manually.';
      clearTimeout(reset);
      reset = setTimeout(() => { button.textContent = 'Copy code'; }, 2000);
    });
  });
  // Mobile navigation collapses before restoring a direct link's position.
  if (window.location.hash) {
    requestAnimationFrame(() => {
      try { focusSection(decodeURIComponent(window.location.hash.slice(1))); } catch { /* Ignore malformed external fragments. */ }
    });
  }
})();
