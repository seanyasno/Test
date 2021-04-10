const express = require('express');
const csv = require('csv-parser');
const dotenv = require('dotenv');
const _ = require('lodash');
const fs = require('fs');

dotenv.config();

let data;
const PORT = process.env.PORT || 4200;

const app = express();

const loadCSVFile = (filePath) => {
    const rows = [];
    return new Promise(resolve => {
        fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (row) => {
            rows.push(row);
        })
        .on('end', () => {
            console.log('CSV file is loaded successfully.');
            resolve(rows);
        });
    });
}

app.get('/data', async (req, res) => {
    res.json(data);
});

app.get('/data/show_ids', async (req, res) => {
    res.json(data.map(item => item.show_id));
});

app.get('/data/types', async (req, res) => {
    res.json(['TV Show', 'Movie']);
});

app.get('/data/titles', async (req, res) => {
    res.json(data.map(item => item.title));
});

app.get('/data/directors', async (req, res) => {
    res.json(_.uniq(data.map(item => item.director)));
});

app.get('/data/countries', async (req, res) => {
    res.json(_.uniq(_.flatMap(_.uniq(data.map(item => item.country.split(', ').map(i => i.replace(',', ''))))).sort()));
});

app.get('/data/categories', async (req, res) => {
    res.json(_.uniq(_.flatMap(_.uniq(data.map(item => item.listed_in.split(', ').map(i => i.replace(',', ''))))).sort()));
});

app.listen(PORT, async () => {
    data = await loadCSVFile(__dirname + '/assets/netflix_titles.csv');
    console.log(`Server is up and running at port ${PORT}`);
    
});