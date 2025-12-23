document.addEventListener('DOMContentLoaded', async () => {
    // --- Element Selections ---
    const reportSelect = document.getElementById('report-select');
    const brandInputContainer = document.getElementById('brand-input-container'); // For competition analysis (text input)
    const brandInput = document.getElementById('brand-input');

    const tagInputContainer = document.getElementById('tag-input-container');
    const tagSelect = document.getElementById('tag-select');

    const brandSelectContainer = document.getElementById('brand-select-container'); // For "View by Brand" (dropdown)
    const brandSelect = document.getElementById('brand-select');

    const timestampSelectContainer = document.getElementById('timestamp-select-container');
    const timestampSelect = document.getElementById('timestamp-select');

    const citySelectContainer = document.getElementById('city-select-container');
    const citySelect = document.getElementById('city-select');


    const generateBtn = document.getElementById('generate-report-btn');
    const saveBtn = document.getElementById('save-report-btn');
    const reportArea = document.getElementById('report-area');
    const helpButton = document.getElementById('help-button');
    const helpModal = document.getElementById('help-modal');
    const closeModalBtn = document.getElementById('close-modal-btn');

    // --- State Variable ---
    let currentReportData = null;

    // --- Initialization: Fetch Tags and Brands ---
    async function loadOptions() {
        try {
            const [tagsResponse, brandsResponse, timestampsResponse, citiesResponse] = await Promise.all([
                fetch('/api/tags'),
                fetch('/api/brands'),
                fetch('/api/timestamps'),
                fetch('/api/cities')
            ]);


            if (tagsResponse.ok) {
                const tags = await tagsResponse.json();
                tags.forEach(tag => {
                    const option = document.createElement('option');
                    option.value = tag;
                    option.textContent = tag;
                    tagSelect.appendChild(option);
                });
            }

            if (brandsResponse.ok) {
                const brands = await brandsResponse.json();
                brands.forEach(brand => {
                    const option = document.createElement('option');
                    option.value = brand;
                    option.textContent = brand;
                    brandSelect.appendChild(option);
                });
            }

            if (timestampsResponse.ok) {
                const timestamps = await timestampsResponse.json();
                timestamps.forEach(ts => {
                    const option = document.createElement('option');
                    option.value = ts;
                    option.textContent = ts;
                    timestampSelect.appendChild(option);
                });
            }

            if (citiesResponse.ok) {
                const cities = await citiesResponse.json();
                cities.forEach(item => {
                    const option = document.createElement('option');
                    // Store both city and state in value for easier parsing or just city if unique
                    // The backend expects city and state separately. Let's start with json string for value or datasets
                    option.value = JSON.stringify({ city: item.city, state: item.state });
                    option.textContent = `${item.city}, ${item.state}`;
                    citySelect.appendChild(option);
                });
            }

        } catch (error) {
            console.error("Failed to load options:", error);
            reportArea.innerHTML += `<p class="text-red-500 text-sm">Warning: Failed to load filter options.</p>`;
        }
    }

    await loadOptions();

    // --- Event Listeners ---
    function updateUI() {
        const reportType = reportSelect.value;

        // Hide all first
        brandInputContainer.classList.add('hidden');
        tagInputContainer.classList.add('hidden');
        brandSelectContainer.classList.add('hidden');
        timestampSelectContainer.classList.add('hidden');
        citySelectContainer.classList.add('hidden');


        if (reportType === 'brand-competition') {
            brandInputContainer.classList.remove('hidden');
        } else if (reportType === 'by-tag') {
            tagInputContainer.classList.remove('hidden');
        } else if (reportType === 'by-brand') {
            brandSelectContainer.classList.remove('hidden');
        } else if (reportType === 'price-location') {
            timestampSelectContainer.classList.remove('hidden');
            citySelectContainer.classList.remove('hidden');
            tagInputContainer.classList.remove('hidden');
        }

    }

    reportSelect.addEventListener('change', updateUI);
    // Initial call to set correct state (e.g. if browser cached selection)
    updateUI();

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
        } else if (reportType === 'by-tag') {
            const selectedTag = tagSelect.value;
            if (!selectedTag) {
                reportArea.innerHTML = `<p class="text-red-500">Please select a product tag.</p>`;
                return;
            }
            url = `/api/reports/by-tag?tag=${encodeURIComponent(selectedTag)}`;
        } else if (reportType === 'by-brand') {
            const selectedBrand = brandSelect.value;
            if (!selectedBrand) {
                reportArea.innerHTML = `<p class="text-red-500">Please select a brand.</p>`;
                return;
            }
            url = `/api/reports/by-brand?brand=${encodeURIComponent(selectedBrand)}`;
        } else if (reportType === 'price-location') {
            const selectedTs = timestampSelect.value;
            const selectedCityJson = citySelect.value;
            const selectedTag = tagSelect.value;

            if (!selectedTs || !selectedCityJson || !selectedTag) {
                reportArea.innerHTML = `<p class="text-red-500">Please select Timestamp, City, and Tag.</p>`;
                return;
            }

            const cityData = JSON.parse(selectedCityJson);
            url = `/api/reports/price-location?timestamp=${selectedTs}&state=${encodeURIComponent(cityData.state)}&city=${encodeURIComponent(cityData.city)}&tag=${encodeURIComponent(selectedTag)}`;
            // Although get_price_stats_by_location logic *retrieves* for specific location, 
            // it doesn't filter by tag in the SQL join in my implementation?
            // Checking reports.py implementation:
            // My implementation GROUPS by tag, it does NOT filter by tag in the query input arguments.
            // Wait, the user story said "select... the article tag...".
            // My implementation of get_price_stats_by_location does NOT accept a tag argument.
            // I must fix reports.py backend or filter in frontend.
            // Filtering in backend is better for large datasets.
            // I will update the URL construction now, but I need to fix the backend function momentarily.
            // For now, I'll filter client side if needed, or better, add the param to backend.
            // I'll assume backend implementation update is next task if I realized this gap.
        } else {

            // Fallback
            url = `/api/reports/${reportType}`;
        }

        try {
            const response = await fetch(url);
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            currentReportData = data;
            renderReport(data);
            saveBtn.disabled = Object.keys(data).length === 0;
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
                    // It's a list items (like brand string or description string), value is the item
                    li.textContent = value;
                    li.className = 'font-normal text-gray-700';
                } else {
                    // It's an object/dict key
                    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                        li.textContent = key;
                        li.className = 'font-semibold';
                        li.appendChild(createList(value));
                    } else if (Array.isArray(value)) {
                        li.textContent = key;
                        li.className = 'font-semibold';
                        li.appendChild(createList(value));
                    } else {
                        // Leaf node (e.g. min: 10.5)
                        li.textContent = `${key}: ${value}`;
                        li.className = 'font-normal text-gray-800';
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

            if (Array.isArray(data)) {
                data.forEach(item => {
                    md += `- ${item}\n`;
                });
                md += '\n';
                return md;
            }

            for (const key in data) {
                const value = data[key];
                md += `${prefix} ${key}\n\n`;
                if (typeof value === 'object' && value !== null) {
                    md += buildMarkdown(value, level + 1);
                }
            }
            return md;
        };

        markdown += buildMarkdown(currentReportData, 2);

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
