const express = require('express');
const router = express.Router();
const { getSingleSportsData } = require('../controllers/getSingleSportsDataController');
const { buildMaterialisedViewControllerForIdScopeResources } = require('../controllers/buildMaterialisedViewController');
const { buildMaterialisedViewControllerForStaff } = require('../controllers/buildMaterialisedViewController');
const { buildMaterialisedViewControllerForKeyMoment } = require('../controllers/buildMaterialisedViewController');

////////////////////////////////////////////////////////////////////////////////
router.get('/:schemaType/:scope/:id', getSingleSportsData);
router.post('/aggregate/:schemaType/:scope/:id', buildMaterialisedViewControllerForIdScopeResources);
router.post('/aggregate/staff/sp/:spScope/:spId/:type/:orgIdScope/:orgId', buildMaterialisedViewControllerForStaff);
router.post('/aggregate/km/:eventIdScope/:eventId/:type/:subType/:dateTime', buildMaterialisedViewControllerForKeyMoment);

////////////////////////////////////////////////////////////////////////////////
module.exports = router;
