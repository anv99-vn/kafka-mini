document.addEventListener('DOMContentLoaded', () => {
    const topicTable = document.getElementById('topic-table').querySelector('tbody');
    const topicCount = document.getElementById('topic-count');
    const statusBadge = document.getElementById('status-badge');
    const modal = document.getElementById('topic-modal');
    
    // Elements for creation
    const btnAddTopic = document.getElementById('btn-add-topic');
    const btnCancel = document.getElementById('btn-cancel');
    const btnConfirmCreate = document.getElementById('btn-confirm-create');
    const inputName = document.getElementById('new-topic-name');
    const inputPartitions = document.getElementById('new-topic-partitions');
    const inputRF = document.getElementById('new-topic-rf');

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

    function renderTopics(topics) {
        topicCount.textContent = topics.length;
        topicTable.innerHTML = '';
        
        if (topics.length === 0) {
            topicTable.innerHTML = '<tr><td colspan="4" style="text-align:center; color: var(--text-dim);">No topics found</td></tr>';
            return;
        }

        topics.forEach(t => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td>${t.name}</td>
                <td>${t.partitions}</td>
                <td>${t.replication_factor}</td>
                <td>
                    <button class="btn danger btn-delete" data-name="${t.name}">Delete</button>
                </td>
            `;
            topicTable.appendChild(tr);
        });

        // Add delete handlers
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
            partitions: parseInt(inputPartitions.value),
            replication_factor: parseInt(inputRF.value)
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

    // Initial load
    loadTopics();
    setInterval(loadTopics, 5000);
});
