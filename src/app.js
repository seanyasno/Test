const express = require('express');
const csv = require('csv-parser');
const dotenv = require('dotenv');
const _ = require('lodash');
const fs = require('fs');
const { lowerCase } = require('lodash');

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

app.get('/data/ratings', async (req, res) => {
    res.json(_.uniq(data.map(item => item.rating).sort()));
});

app.get('/data/filter/:filter/:value', async (req, res) => {
    const {filter = '', value = ''} = req.params;
    const filters = ['type', 'title', 'director', 'country', 'release_year', 'rating', 'duration'];

    if (_.isEmpty(filter)) {
        return res.status(406).json({error: 'Filter cannot be empty.'});
    } else if (_.isEmpty(value)) {
        return res.status(406).json({error: 'Value cannot be empty.'});
    } else if (filters.find(item => item === filter)) {
        return res.json(data.filter(item => lowerCase(item[filter]) === lowerCase(value)));
    } else {
        return res.status(406).json({error: 'Wrong filter.'});
    }
});

app.listen(PORT, async () => {
    data = await loadCSVFile(__dirname + '/assets/netflix_titles.csv');
    console.log(`Server is up and running at port ${PORT}`);
});