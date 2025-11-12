document.addEventListener('DOMContentLoaded', () => {
    // --- Element Selections ---
    const reportSelect = document.getElementById('report-select');
    const brandInputContainer = document.getElementById('brand-input-container');
    const brandInput = document.getElementById('brand-input');
    const generateBtn = document.getElementById('generate-report-btn');
    const saveBtn = document.getElementById('save-report-btn');
    const reportArea = document.getElementById('report-area');
    const helpButton = document.getElementById('help-button');
    const helpModal = document.getElementById('help-modal');
    const closeModalBtn = document.getElementById('close-modal-btn');

    // --- State Variable ---
    let currentReportData = null; // To store the latest generated report data

    // --- Event Listeners ---
    reportSelect.addEventListener('change', () => {
        brandInputContainer.classList.toggle('hidden', reportSelect.value !== 'brand-competition');
    });

    generateBtn.addEventListener('click', generateReport);
    saveBtn.addEventListener('click', saveReportAsMarkdown);
    helpButton.addEventListener('click', () => helpModal.classList.remove('hidden'));
    closeModalBtn.addEventListener('click', () => helpModal.classList.add('hidden'));
    window.addEventListener('click', (event) => {
        if (event.target === helpModal) {
            helpModal.classList.add('hidden');
        }
    });

    // --- Core Functions ---
    async function generateReport() {
        const reportType = reportSelect.value;
        let url = '';

        reportArea.innerHTML = `<p class="text-gray-500">Generating report...</p>`;
        saveBtn.disabled = true;
        currentReportData = null;

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

        try {
            const response = await fetch(url);
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            currentReportData = data; // Store data for saving
            renderReport(data);
            saveBtn.disabled = Object.keys(data).length === 0; // Enable save if data exists
        } catch (error) {
            reportArea.innerHTML = `<p class="text-red-500">Error: ${error.message}</p>`;
        }
    }

    function renderReport(data) {
        if (!data || Object.keys(data).length === 0) {
            reportArea.innerHTML = `<p class="text-gray-500">No data found for the selected report.</p>`;
            return;
        }

        const createList = (currentData) => {
            const ul = document.createElement('ul');
            ul.className = 'list-disc ml-5 space-y-1';
            for (const key in currentData) {
                const value = currentData[key];
                const li = document.createElement('li');
                if (Array.isArray(currentData)) {
                    li.textContent = value;
                    li.className = 'font-normal text-gray-700';
                } else {
                    li.textContent = key;
                    li.className = 'font-semibold';
                    if (typeof value === 'object' && value !== null) {
                        li.appendChild(createList(value));
                    }
                }
                ul.appendChild(li);
            }
            return ul;
        };

        reportArea.innerHTML = '';
        reportArea.appendChild(createList(data));
    }

    function saveReportAsMarkdown() {
        if (!currentReportData) {
            alert("No report data to save.");
            return;
        }

        const selectedOption = reportSelect.options[reportSelect.selectedIndex].text;
        let markdown = `# ${selectedOption}\n\n`;

        const buildMarkdown = (data, level) => {
            let md = '';
            const prefix = '#'.repeat(level);

            // Handle bullet lists (arrays of strings)
            if (Array.isArray(data)) {
                data.forEach(item => {
                    md += `- ${item}\n`;
                });
                md += '\n'; // Add the extra newline character after the entire list
                return md;
            }

            // Handle headings (objects)
            for (const key in data) {
                const value = data[key];
                md += `${prefix} ${key}\n\n`; // Add extra space after headings
                if (typeof value === 'object' && value !== null) {
                    md += buildMarkdown(value, level + 1);
                }
            }
            return md;
        };

        markdown += buildMarkdown(currentReportData, 2);

        // Create a downloadable file
        const blob = new Blob([markdown], { type: 'text/markdown;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `hermes-report-${reportSelect.value}.md`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }
});
