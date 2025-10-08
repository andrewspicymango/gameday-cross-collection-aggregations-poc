const express = require('express');
const router = express.Router();
const { getSingleSportsData } = require('../controllers/getSingleSportsDataController');
const { buildMaterialisedViewController } = require('../controllers/buildMaterialisedViewController');

////////////////////////////////////////////////////////////////////////////////
router.get('/:schemaType/:scope/:id', getSingleSportsData);
router.post('/aggregate/:schemaType/:scope/:id', buildMaterialisedViewController);

////////////////////////////////////////////////////////////////////////////////
module.exports = router;
