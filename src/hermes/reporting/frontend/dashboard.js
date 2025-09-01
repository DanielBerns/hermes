document.addEventListener('DOMContentLoaded', () => {
    const reportSelect = document.getElementById('report-select');
    const brandInputContainer = document.getElementById('brand-input-container');
    const brandInput = document.getElementById('brand-input');
    const generateBtn = document.getElementById('generate-report-btn');
    const reportArea = document.getElementById('report-area');

    // Help Modal elements
    const helpButton = document.getElementById('help-button');
    const helpModal = document.getElementById('help-modal');
    const closeModalBtn = document.getElementById('close-modal-btn');

    // --- Event Listeners ---

    // Toggle brand input visibility based on report selection
    reportSelect.addEventListener('change', () => {
        if (reportSelect.value === 'brand-competition') {
            brandInputContainer.classList.remove('hidden');
        } else {
            brandInputContainer.classList.add('hidden');
        }
    });

    // Generate report on button click
    generateBtn.addEventListener('click', async () => {
        const reportType = reportSelect.value;
        let url = '';

        reportArea.innerHTML = `<p class="text-gray-500">Generating report...</p>`;

        // Construct the API URL
        if (reportType === 'brand-competition') {
            const brandName = brandInput.value.trim();
            if (!brandName) {
                reportArea.innerHTML = `<p class="text-red-500">Please enter a brand name.</p>`;
                return;
            }
            url = `/api/reports/brand-competition/${encodeURIComponent(brandName)}`;
        } else {
            url = `/api/reports/${reportType}`;
        }

        // Fetch data and render it
        try {
            const response = await fetch(url);
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            renderReport(data);
        } catch (error) {
            reportArea.innerHTML = `<p class="text-red-500">Error: ${error.message}</p>`;
        }
    });

    // Help modal listeners
    helpButton.addEventListener('click', () => helpModal.classList.remove('hidden'));
    closeModalBtn.addEventListener('click', () => helpModal.classList.add('hidden'));
    window.addEventListener('click', (event) => {
        if (event.target == helpModal) {
            helpModal.classList.add('hidden');
        }
    });

    // --- Helper Functions ---

    /**
     * Renders the report data into the reportArea element.
     * @param {object} data - The report data from the API.
     */
    function renderReport(data) {
        if (!data || Object.keys(data).length === 0) {
            reportArea.innerHTML = `<p class="text-gray-500">No data found for the selected report.</p>`;
            return;
        }

        /**
         * Recursively builds a nested <ul> list from an object or array.
         * @param {object|array} currentData - The current level of data to render.
         * @returns {HTMLUListElement} The generated list element.
         */
        const createList = (currentData) => {
            const ul = document.createElement('ul');
            ul.className = 'list-disc ml-5 space-y-1';

            // Use a single loop that works for both objects (keys) and arrays (indices)
            for (const key in currentData) {
                const value = currentData[key];
                const li = document.createElement('li');

                if (Array.isArray(currentData)) {
                    // If it's an array, the 'value' is the product string we want to display.
                    li.textContent = value;
                    li.className = 'font-normal text-gray-700';
                } else {
                    // If it's an object, the 'key' is the tag/brand name.
                    li.textContent = key;
                    li.className = 'font-semibold';
                    // If the value is an object or array, recurse to build the next level.
                    if (typeof value === 'object' && value !== null) {
                        li.appendChild(createList(value));
                    }
                }
                ul.appendChild(li);
            }
            return ul;
        };

        reportArea.innerHTML = ''; // Clear previous content
        reportArea.appendChild(createList(data));
    }
});
