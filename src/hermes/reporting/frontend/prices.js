document.addEventListener('DOMContentLoaded', async () => {
    const titleEl = document.getElementById('article-title');
    const codeDisplayEl = document.getElementById('article-code-display');
    const tbody = document.getElementById('prices-tbody');
    const noDataMessage = document.getElementById('no-data-message');

    // Extract code from URL query parameters (?code=XYZ)
    const params = new URLSearchParams(window.location.search);
    const articleCode = params.get('code');

    if (!articleCode) {
        titleEl.textContent = 'Error: No article code provided';
        titleEl.classList.add('text-red-500');
        return;
    }

    titleEl.textContent = `Price History`;
    codeDisplayEl.textContent = `Article Code: ${articleCode}`;

    // Format currency: divide by 100, append '$' and force 2 decimal digits
    function formatPrice(amount) {
        return '$ ' + (amount / 100).toFixed(2);
    }

    // Format ISO string into a localized, readable date/time
    function formatTimestamp(isoString) {
        const date = new Date(isoString);
        return date.toLocaleString();
    }

    try {
        const response = await fetch(`/api/prices/${encodeURIComponent(articleCode)}`);

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();

        if (data.length === 0) {
            noDataMessage.classList.remove('hidden');
            return;
        }

        // Populate table efficiently
        const fragment = document.createDocumentFragment();
        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.className = 'hover:bg-gray-50';

            const tdCity = document.createElement('td');
            tdCity.className = 'px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-medium';
            tdCity.textContent = row.city;

            const tdTime = document.createElement('td');
            tdTime.className = 'px-6 py-4 whitespace-nowrap text-sm text-gray-500';
            tdTime.textContent = formatTimestamp(row.timestamp);

            const tdMin = document.createElement('td');
            tdMin.className = 'px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-medium text-green-600';
            tdMin.textContent = formatPrice(row.min_price);

            const tdMax = document.createElement('td');
            tdMax.className = 'px-6 py-4 whitespace-nowrap text-sm text-gray-900 font-medium text-red-600';
            tdMax.textContent = formatPrice(row.max_price);

            // Appended in the new sorted order (City -> Timestamp)
            tr.appendChild(tdCity);
            tr.appendChild(tdTime);
            tr.appendChild(tdMin);
            tr.appendChild(tdMax);

            fragment.appendChild(tr);
        });

        tbody.appendChild(fragment);

    } catch (error) {
        console.error("Failed to fetch prices:", error);
        titleEl.textContent = 'Error loading price history';
        titleEl.classList.add('text-red-500');
        codeDisplayEl.textContent = error.message;
    }
});
