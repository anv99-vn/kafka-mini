document.addEventListener('DOMContentLoaded', () => {
    const topicTable = document.getElementById('topic-table').querySelector('tbody');
    const groupTable = document.getElementById('group-table').querySelector('tbody');
    const topicCount = document.getElementById('topic-count');
    const groupCount = document.getElementById('group-count');
    const statusBadge = document.getElementById('status-badge');
    const modal = document.getElementById('topic-modal');
    
    // Node Info elements
    const nodeBadge = document.getElementById('node-badge');
    const nodeId = document.getElementById('node-id');
    const nodeState = document.getElementById('node-state');
    const nodeTerm = document.getElementById('node-term');
    const nodeLeader = document.getElementById('node-leader');

    // Elements for creation
    const btnAddTopic = document.getElementById('btn-add-topic');
    const btnCancel = document.getElementById('btn-cancel');
    const btnConfirmCreate = document.getElementById('btn-confirm-create');
    const inputName = document.getElementById('new-topic-name');
    const inputPartitions = document.getElementById('new-topic-partitions');

    async function fetchData() {
        await Promise.all([
            loadTopics(),
            loadStatus(),
            loadGroups()
        ]);
    }

    async function loadTopics() {
        try {
            const resp = await fetch('/api/topics');
            if (resp.ok) {
                const topics = await resp.json();
                renderTopics(topics);
                statusBadge.textContent = 'Broker Online';
                statusBadge.className = 'badge online';
            } else {
                throw new Error('API Error');
            }
        } catch (err) {
            statusBadge.textContent = 'Service Offline';
            statusBadge.className = 'badge';
        }
    }

    async function loadStatus() {
        try {
            const resp = await fetch('/api/status');
            if (resp.ok) {
                const status = await resp.json();
                nodeId.textContent = status.node_id;
                nodeState.textContent = status.state;
                nodeTerm.textContent = status.term;
                nodeLeader.textContent = status.leader_id;
                
                nodeBadge.textContent = status.state;
                nodeBadge.className = `badge ${status.state.toLowerCase()}`;
            }
        } catch (err) {
            console.error('Failed to load status', err);
        }
    }

    async function loadGroups() {
        try {
            const resp = await fetch('/api/groups');
            if (resp.ok) {
                const groups = await resp.json();
                renderGroups(groups);
            }
        } catch (err) {
            console.error('Failed to load groups', err);
        }
    }

    function renderTopics(topics) {
        topicCount.textContent = topics.length;
        topicTable.innerHTML = '';
        
        if (topics.length === 0) {
            topicTable.innerHTML = '<tr><td colspan="3" style="text-align:center; color: var(--text-dim);">No topics found</td></tr>';
            return;
        }

        topics.forEach(t => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${t.name}</td>
                <td>${t.partitions}</td>
                <td>
                    <button class="btn danger btn-delete" data-name="${t.name}">Delete</button>
                </td>
            `;
            topicTable.appendChild(tr);
        });

        document.querySelectorAll('.btn-delete').forEach(btn => {
            btn.onclick = async () => {
                const name = btn.dataset.name;
                if (confirm(`Delete topic "${name}"?`)) {
                    const resp = await fetch(`/api/topics/${name}`, { method: 'DELETE' });
                    if (resp.ok) loadTopics();
                }
            };
        });
    }

    function renderGroups(groups) {
        groupCount.textContent = groups.length;
        groupTable.innerHTML = '';

        if (groups.length === 0) {
            groupTable.innerHTML = '<tr><td colspan="4" style="text-align:center; color: var(--text-dim);">No active groups</td></tr>';
            return;
        }

        groups.forEach(g => {
            const tr = document.createElement('tr');
            const topicsTags = g.topics.map(t => `<span class="tag">${t}</span>`).join('');
            tr.innerHTML = `
                <td>${g.name}</td>
                <td>${topicsTags}</td>
                <td>${g.members}</td>
                <td><span class="badge ${g.state === 'Stable' ? 'online' : ''}">${g.state}</span></td>
            `;
            groupTable.appendChild(tr);
        });
    }

    btnAddTopic.onclick = () => {
        modal.style.display = 'flex';
    };

    btnCancel.onclick = () => {
        modal.style.display = 'none';
        inputName.value = '';
    };

    btnConfirmCreate.onclick = async () => {
        const body = {
            name: inputName.value,
            partitions: parseInt(inputPartitions.value)
        };

        if (!body.name) return alert('Name is required');

        const resp = await fetch('/api/topics', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });

        if (resp.ok) {
            modal.style.display = 'none';
            inputName.value = '';
            loadTopics();
        } else {
            alert('Failed to create topic');
        }
    };

    fetchData();
    setInterval(fetchData, 3000);
});
