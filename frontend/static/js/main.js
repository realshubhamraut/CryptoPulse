/* ============================================================================
   CryptoPulse — Frontend JavaScript
   Live data, Chart.js, SSE, agent analysis
   ============================================================================ */

// =============================================================================
// Utility Helpers
// =============================================================================

function formatPrice(price) {
    if (price >= 1000) return price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (price >= 1) return price.toFixed(4);
    return price.toFixed(6);
}

function formatVolume(vol) {
    if (vol >= 1e9) return (vol / 1e9).toFixed(2) + 'B';
    if (vol >= 1e6) return (vol / 1e6).toFixed(2) + 'M';
    if (vol >= 1e3) return (vol / 1e3).toFixed(1) + 'K';
    return vol.toFixed(0);
}

function formatNumber(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return n.toString();
}

function timeAgo(ts) {
    const diff = Math.floor(Date.now() / 1000) - ts;
    if (diff < 60) return 'just now';
    if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
    if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
    return Math.floor(diff / 86400) + 'd ago';
}


// =============================================================================
// Price Loading (Dashboard)
// =============================================================================

let performanceChart = null;

async function loadPrices() {
    try {
        const resp = await fetch('/api/prices');
        const data = await resp.json();

        let totalVolume = 0;
        let totalTrades = 0;
        let changes = [];
        let labels = [];
        let chartChanges = [];
        let chartColors = [];

        for (const [symbol, info] of Object.entries(data)) {
            const priceEl = document.getElementById(`price-${symbol}`);
            const changeEl = document.getElementById(`change-${symbol}`);

            if (priceEl) {
                priceEl.textContent = '$' + formatPrice(info.price);
                priceEl.style.color = info.change >= 0 ? 'var(--accent-green)' : 'var(--accent-red)';
            }

            if (changeEl) {
                const arrow = info.change >= 0 ? '▲' : '▼';
                changeEl.textContent = `${arrow} ${Math.abs(info.change).toFixed(2)}%`;
                changeEl.className = 'ticker-change ' + (info.change >= 0 ? 'positive' : 'negative');
            }

            totalVolume += info.volume || 0;
            totalTrades += info.trades || 0;
            changes.push(info.change);

            // For chart
            const coinMeta = { BTCUSDT: 'BTC', ETHUSDT: 'ETH', BNBUSDT: 'BNB', SOLUSDT: 'SOL', XRPUSDT: 'XRP', ADAUSDT: 'ADA', DOGEUSDT: 'DOGE', AVAXUSDT: 'AVAX' };
            labels.push(coinMeta[symbol] || symbol);
            chartChanges.push(info.change);
            chartColors.push(info.change >= 0 ? 'rgba(16, 185, 129, 0.7)' : 'rgba(239, 68, 68, 0.7)');
        }

        // Update stats
        const volEl = document.getElementById('total-volume');
        const tradeEl = document.getElementById('total-trades');
        const sentEl = document.getElementById('market-sentiment');

        if (volEl) volEl.textContent = '$' + formatVolume(totalVolume);
        if (tradeEl) tradeEl.textContent = formatNumber(totalTrades);

        if (sentEl) {
            const avg = changes.reduce((a, b) => a + b, 0) / (changes.length || 1);
            if (avg > 1) {
                sentEl.textContent = '🟢 Bullish';
                sentEl.style.color = 'var(--accent-green)';
            } else if (avg < -1) {
                sentEl.textContent = '🔴 Bearish';
                sentEl.style.color = 'var(--accent-red)';
            } else {
                sentEl.textContent = '🟡 Neutral';
                sentEl.style.color = 'var(--accent-amber)';
            }
        }

        // Performance chart
        const chartEl = document.getElementById('performance-chart');
        if (chartEl) {
            if (performanceChart) performanceChart.destroy();
            performanceChart = new Chart(chartEl, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [{
                        label: '24h Change %',
                        data: chartChanges,
                        backgroundColor: chartColors,
                        borderRadius: 6,
                        borderSkipped: false,
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                    },
                    scales: {
                        x: {
                            ticks: { color: '#94a3b8', font: { family: 'JetBrains Mono', size: 11 } },
                            grid: { display: false },
                        },
                        y: {
                            ticks: {
                                color: '#64748b',
                                font: { family: 'JetBrains Mono', size: 11 },
                                callback: (v) => v + '%'
                            },
                            grid: { color: 'rgba(255,255,255,0.04)' },
                        }
                    }
                }
            });
        }
    } catch (e) {
        console.error('Price load failed:', e);
    }
}


// =============================================================================
// News Loading
// =============================================================================

let allArticles = [];

async function loadNews() {
    try {
        const resp = await fetch('/api/news');
        const data = await resp.json();
        allArticles = data.articles || [];
        renderNews(allArticles);
        updateSentimentChart(allArticles);
    } catch (e) {
        console.error('News load failed:', e);
    }
}

async function loadDashboardNews() {
    try {
        const resp = await fetch('/api/news');
        const data = await resp.json();
        const articles = (data.articles || []).slice(0, 5);
        const container = document.getElementById('dashboard-news');
        if (!container) return;

        container.innerHTML = articles.map(a => `
            <div class="news-item">
                <div class="sentiment-dot ${a.sentiment}"></div>
                <div style="flex:1;">
                    <h4><a href="${a.url}" target="_blank">${a.title}</a></h4>
                    <div class="news-meta">
                        <span>${a.source}</span>
                        <span>•</span>
                        <span>${timeAgo(a.published)}</span>
                        <span class="badge badge-${a.sentiment === 'positive' ? 'green' : a.sentiment === 'negative' ? 'red' : 'blue'}">${a.sentiment}</span>
                    </div>
                </div>
            </div>
        `).join('');
    } catch (e) {
        console.error('Dashboard news failed:', e);
    }
}

function renderNews(articles) {
    const container = document.getElementById('news-feed');
    if (!container) return;

    if (articles.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="icon">📰</div><h3>No articles found</h3></div>';
        return;
    }

    // Update counters
    const posCount = articles.filter(a => a.sentiment === 'positive').length;
    const negCount = articles.filter(a => a.sentiment === 'negative').length;
    const neuCount = articles.filter(a => a.sentiment === 'neutral').length;

    const posEl = document.getElementById('positive-count');
    const negEl = document.getElementById('negative-count');
    const neuEl = document.getElementById('neutral-count');
    if (posEl) posEl.textContent = posCount + ' Positive';
    if (negEl) negEl.textContent = negCount + ' Negative';
    if (neuEl) neuEl.textContent = neuCount + ' Neutral';

    container.innerHTML = articles.map(a => `
        <div class="news-item" data-source="${(a.source || '').toLowerCase()}">
            <div class="sentiment-dot ${a.sentiment}"></div>
            <div style="flex:1;">
                <h4><a href="${a.url}" target="_blank">${a.title}</a></h4>
                <p style="font-size:12px; color:var(--text-muted); margin:6px 0; line-height:1.5;">${a.body || ''}</p>
                <div class="news-meta">
                    <span style="font-weight:600;">${a.source}</span>
                    <span>•</span>
                    <span>${timeAgo(a.published)}</span>
                    ${(a.categories || []).map(c => `<span class="badge badge-blue" style="font-size:9px;">${c.trim()}</span>`).join('')}
                    <span class="badge badge-${a.sentiment === 'positive' ? 'green' : a.sentiment === 'negative' ? 'red' : 'blue'}">${a.sentiment}</span>
                </div>
            </div>
        </div>
    `).join('');
}

function filterNews(source, btn) {
    document.querySelectorAll('.source-filters .tab').forEach(t => t.classList.remove('active'));
    btn.classList.add('active');

    if (source === 'all') {
        renderNews(allArticles);
    } else {
        const filtered = allArticles.filter(a => (a.source || '').toLowerCase().includes(source));
        renderNews(filtered);
    }
}

let sentimentChart = null;

function updateSentimentChart(articles) {
    const el = document.getElementById('sentiment-chart');
    if (!el) return;

    const pos = articles.filter(a => a.sentiment === 'positive').length;
    const neg = articles.filter(a => a.sentiment === 'negative').length;
    const neu = articles.filter(a => a.sentiment === 'neutral').length;

    if (sentimentChart) sentimentChart.destroy();
    sentimentChart = new Chart(el, {
        type: 'doughnut',
        data: {
            labels: ['Positive', 'Negative', 'Neutral'],
            datasets: [{
                data: [pos, neg, neu],
                backgroundColor: ['rgba(16,185,129,0.8)', 'rgba(239,68,68,0.8)', 'rgba(59,130,246,0.8)'],
                borderWidth: 0,
                hoverOffset: 8,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '65%',
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: '#94a3b8', font: { size: 12, family: 'Inter' }, padding: 16 }
                }
            }
        }
    });
}


// =============================================================================
// SSE News Stream
// =============================================================================

function connectNewsSSE() {
    const sseDot = document.getElementById('sse-dot');
    const sseStatus = document.getElementById('sse-status');

    try {
        const evtSource = new EventSource('/api/news/stream');

        evtSource.onopen = () => {
            if (sseDot) sseDot.style.background = 'var(--accent-green)';
            if (sseStatus) sseStatus.textContent = 'Live — streaming';
        };

        evtSource.onmessage = (e) => {
            try {
                const article = JSON.parse(e.data);
                // Prepend to feed
                const container = document.getElementById('news-feed');
                if (container) {
                    const div = document.createElement('div');
                    div.className = 'news-item';
                    div.style.animation = 'slideIn 0.5s ease';
                    div.innerHTML = `
                        <div class="sentiment-dot ${article.sentiment}"></div>
                        <div style="flex:1;">
                            <h4><a href="${article.url}" target="_blank">${article.title}</a></h4>
                            <div class="news-meta">
                                <span style="font-weight:600;">${article.source}</span>
                                <span>•</span>
                                <span>${timeAgo(article.published)}</span>
                                <span class="badge badge-${article.sentiment === 'positive' ? 'green' : article.sentiment === 'negative' ? 'red' : 'blue'}">${article.sentiment}</span>
                                <span class="badge badge-purple">NEW</span>
                            </div>
                        </div>
                    `;
                    container.prepend(div);
                }
            } catch (err) {
                console.error('SSE parse error:', err);
            }
        };

        evtSource.onerror = () => {
            if (sseDot) sseDot.style.background = 'var(--accent-amber)';
            if (sseStatus) sseStatus.textContent = 'Reconnecting...';
        };
    } catch (e) {
        if (sseDot) sseDot.style.background = 'var(--accent-red)';
        if (sseStatus) sseStatus.textContent = 'Disconnected';
    }
}


// =============================================================================
// Agent Analysis
// =============================================================================

let probChart = null;

async function runAnalysis() {
    const symbolEl = document.getElementById('symbol-select');
    const horizonEl = document.getElementById('horizon-select');
    const btn = document.getElementById('analyze-btn');
    const statusBadge = document.getElementById('pipeline-status');

    const symbol = symbolEl ? symbolEl.value : 'BTCUSDT';
    const horizon = horizonEl ? horizonEl.value : '5';

    // Show loading
    btn.disabled = true;
    btn.innerHTML = '<div class="spinner" style="width:16px;height:16px;border-width:2px;"></div> Analyzing...';
    statusBadge.textContent = 'Running';
    statusBadge.className = 'badge badge-amber';

    // Animate pipeline stages
    const stages = ['stage-market', 'stage-sentiment', 'stage-risk', 'stage-portfolio'];
    stages.forEach(s => {
        const el = document.getElementById(s);
        if (el) el.style.borderColor = 'var(--border-color)';
    });

    // Simulate stage progress
    let stageIdx = 0;
    const stageInterval = setInterval(() => {
        if (stageIdx < stages.length) {
            const el = document.getElementById(stages[stageIdx]);
            if (el) {
                el.style.borderColor = 'var(--accent-blue)';
                el.style.boxShadow = 'var(--shadow-glow-blue)';
            }
            stageIdx++;
        }
    }, 1500);

    try {
        const resp = await fetch(`/api/analyze/${symbol}?horizon=${horizon}`, { method: 'POST' });
        const data = await resp.json();

        clearInterval(stageInterval);

        // Light up all stages
        stages.forEach(s => {
            const el = document.getElementById(s);
            if (el) {
                el.style.borderColor = 'var(--accent-green)';
                el.style.boxShadow = 'var(--shadow-glow-green)';
            }
        });

        statusBadge.textContent = 'Complete';
        statusBadge.className = 'badge badge-green';

        // Show results
        document.getElementById('results-area').style.display = 'grid';
        document.getElementById('empty-state').style.display = 'none';

        // Determine action from recommendation text
        const recText = (data.recommendation || '').toLowerCase();
        let action = 'HOLD';
        let actionClass = 'hold';
        if (recText.includes('buy') || recText.includes('long')) {
            action = 'BUY';
            actionClass = 'buy';
        } else if (recText.includes('sell') || recText.includes('short')) {
            action = 'SELL';
            actionClass = 'sell';
        }

        const recHeader = document.getElementById('rec-header');
        recHeader.className = 'recommendation-header ' + actionClass;

        document.getElementById('rec-action').textContent = action;
        document.getElementById('rec-symbol').textContent = symbol;
        document.getElementById('rec-confidence').textContent = ((data.confidence || 0) * 100).toFixed(0) + '%';
        document.getElementById('rec-text').textContent = data.recommendation || 'No recommendation generated.';

        // Agent trace
        const traceEl = document.getElementById('agent-trace');
        if (traceEl && data.agent_trace) {
            const agentClasses = ['market', 'sentiment', 'risk', 'portfolio'];
            const agentIcons = ['📈', '💬', '🛡️', '🎯'];
            traceEl.innerHTML = data.agent_trace.map((t, i) => `
                <div class="agent-card ${agentClasses[i] || ''}">
                    <h4>${agentIcons[i] || '🤖'} ${t.split(':')[0]}</h4>
                    <div class="output">${t}</div>
                </div>
            `).join('');
        }

        // Prediction details
        if (data.prediction) {
            document.getElementById('pred-direction').textContent = data.prediction.direction || '—';
            document.getElementById('pred-confidence').textContent = ((data.prediction.confidence || 0) * 100).toFixed(0) + '%';
            document.getElementById('pred-model').textContent = data.prediction.model_version || '—';
        }
        document.getElementById('pred-latency').textContent = (data.latency_ms || 0).toFixed(0) + ' ms';

        // Probability chart
        if (data.prediction && data.prediction.probabilities) {
            const probs = data.prediction.probabilities;
            const probEl = document.getElementById('prob-chart');
            if (probEl) {
                if (probChart) probChart.destroy();
                probChart = new Chart(probEl, {
                    type: 'bar',
                    data: {
                        labels: Object.keys(probs),
                        datasets: [{
                            data: Object.values(probs),
                            backgroundColor: ['rgba(16,185,129,0.7)', 'rgba(239,68,68,0.7)', 'rgba(59,130,246,0.7)'],
                            borderRadius: 6,
                        }]
                    },
                    options: {
                        indexAxis: 'y',
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { display: false } },
                        scales: {
                            x: {
                                max: 1,
                                ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 11 } },
                                grid: { color: 'rgba(255,255,255,0.04)' },
                            },
                            y: {
                                ticks: { color: '#94a3b8', font: { family: 'JetBrains Mono', size: 12, weight: 'bold' } },
                                grid: { display: false },
                            }
                        }
                    }
                });
            }
        }

    } catch (e) {
        clearInterval(stageInterval);
        statusBadge.textContent = 'Error';
        statusBadge.className = 'badge badge-red';
        alert('Analysis failed: ' + e.message);
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i data-lucide="play" style="width:16px;height:16px;"></i> Run Analysis';
        lucide.createIcons();
    }
}


// =============================================================================
// Health Check
// =============================================================================

async function checkHealth() {
    const el = document.getElementById('api-status');
    try {
        const resp = await fetch('/api/health');
        if (resp.ok) {
            el.innerHTML = '<span class="status-dot"></span><span>API Connected</span>';
        } else {
            el.innerHTML = '<span class="status-dot" style="background:var(--accent-red);"></span><span>API Error</span>';
        }
    } catch (e) {
        el.innerHTML = '<span class="status-dot" style="background:var(--accent-red);"></span><span>API Offline</span>';
    }
}

// Check health on load
checkHealth();
setInterval(checkHealth, 30000);
