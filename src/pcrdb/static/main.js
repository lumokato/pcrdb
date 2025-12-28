// Tab switching
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

        btn.classList.add('active');
        document.getElementById(btn.dataset.tab).classList.add('active');
    });
});

// Format Rank 
function formatRank(rank, isEstimate) {
    if (!rank) return '-';
    // If rank is estimate, append *
    return isEstimate ? `${rank}*` : rank;
}

// Format Number
function formatNum(num) {
    return num ? num.toLocaleString() : '-';
}

// Clear table
function clearTable(tableId) {
    const tbody = document.querySelector(`#${tableId} tbody`);
    tbody.innerHTML = '';
    return tbody;
}

// Add row
function addRow(tbody, cells) {
    const tr = document.createElement('tr');
    cells.forEach(cell => {
        const td = document.createElement('td');
        td.textContent = cell;
        tr.appendChild(td);
    });
    tbody.appendChild(tr);
}

// Show info
function showInfo(divId, text) {
    const div = document.getElementById(divId);
    div.textContent = text;
    div.classList.remove('hidden');
}

// API Calls

async function searchClanHistory() {
    const input = document.getElementById('clan-input').value.trim();
    if (!input) return;

    let url = `/api/clan/history?name=${encodeURIComponent(input)}`;
    // Check if input is purely numeric (assume ID)
    if (/^\d+$/.test(input)) {
        // Try as ID first if clearly numeric, or backend handles name/id check
        // Backend logic: name=... checks name. id=... checks id.
        // Our server code: if name passed, check name. if id passed, check id.
        // Let's deduce:
        url = `/api/clan/history?name=${encodeURIComponent(input)}`;
        // NOTE: The backend logic prioritizes ID if both present but we only pass one query param usually. 
        // Let's modify logic: if numeric, pass as ID? Or let backend handle "name" as string 
        // Our backend implementation: get_clan_history(clan_id=id, clan_name=name).
        // If I pass name="3388", it will search clan_name="3388".
        // If the user meant ID, they should use ID. But user input is mixed.
        // To be safe, if input looks like ID, pass both or handle in frontend?
        // Let's try passing as name first, as the backend `get_clan_history` 
        // handles "input name resolves to ID" logic internally if we pass name.
        // Wait, backend `get_clan_history` takes `clan_id` or `clan_name`. 
        // If `clan_name` is passed, it finds the ID from name.
        // If user inputs "42877" (ID), searching by name "42877" might fail if no clan is NAMED "42877".
        // So simple heuristic:
        if (/^\d{3,6}$/.test(input)) { // Assume ID is 3-6 digits? 
            // Actually, some names are numbers. So passing as name is safer if we want to cover both, 
            // BUT `get_clan_history` query uses `clan_name = %s`.
            // So if I want to search by ID, I MUST pass `id` param.
            url = `/api/clan/history?id=${input}`;
        }
    }

    try {
        const res = await fetch(url);
        const data = await res.json();

        if (data.error) {
            alert(data.error);
            return;
        }

        showInfo('clan-info', `公会: ${data.clan_name || 'Unknown'} (ID: ${data.clan_id})`);

        const tbody = clearTable('clan-history-table');
        data.history.forEach(item => {
            addRow(tbody, [
                item.period,
                formatRank(item.ranking, item.is_estimate),
                item.member_num,
                item.clan_name,
                item.leader_name,
                item.leader_viewer_id
            ]);
        });
    } catch (e) {
        console.error(e);
        alert('查询失败');
    }
}

async function searchPlayerHistory() {
    const vid = document.getElementById('player-input').value.trim();
    if (!vid) return;

    try {
        const res = await fetch(`/api/player/history?vid=${vid}`);
        const data = await res.json();

        showInfo('player-info', `玩家: ${data.user_name || 'Unknown'} (ID: ${data.viewer_id})`);

        const tbody = clearTable('player-history-table');
        data.history.forEach(item => {
            addRow(tbody, [
                item.period,
                item.clan_name,
                formatRank(item.clan_ranking),
                item.player_name || '-',
                item.level,
                formatNum(item.total_power)
            ]);
        });
    } catch (e) {
        console.error(e);
        alert('查询失败');
    }
}

async function loadPowerRanking() {
    try {
        const res = await fetch('/api/clan/power-rank');
        const data = await res.json();

        const tbody = clearTable('power-rank-table');
        data.forEach(item => {
            addRow(tbody, [
                item.rank,
                item.clan_name,
                item.clan_id,
                formatNum(item.avg_power),
                item.member_count
            ]);
        });
    } catch (e) {
        console.error(e);
        alert('加载失败');
    }
}

async function loadGrandWinning() {
    const group = document.getElementById('grand-group').value || 0;
    try {
        const res = await fetch(`/api/grand/winning?group=${group}`);
        const data = await res.json();

        const tbody = clearTable('grand-winning-table');
        data.forEach(item => {
            addRow(tbody, [
                item.rank,
                item.user_name,
                item.viewer_id,
                formatNum(item.winning_number),
                item.grand_arena_rank,
                item.grand_arena_group
            ]);
        });
    } catch (e) {
        console.error(e);
        alert('加载失败');
    }
}
