const express = require('express');
const router = express.Router();
const { getSingleSportsData } = require('../controllers/getSingleSportsDataController');
const { buildMaterialisedViewControllerForIdScopeResources } = require('../controllers/buildMaterialisedViewController');
const { buildMaterialisedViewControllerForTeamStaff } = require('../controllers/buildMaterialisedViewController');
const { buildMaterialisedViewControllerForClubStaff } = require('../controllers/buildMaterialisedViewController');

////////////////////////////////////////////////////////////////////////////////
router.get('/:schemaType/:scope/:id', getSingleSportsData);
router.post('/aggregate/:schemaType/:scope/:id', buildMaterialisedViewControllerForIdScopeResources);
router.post('/aggregate/staff/sp/:spScope/:spId/team/:teamIdScope/:teamId', buildMaterialisedViewControllerForTeamStaff);
router.post('/aggregate/staff/sp/:spScope/:spId/club/:clubIdScope/:clubId', buildMaterialisedViewControllerForClubStaff);

////////////////////////////////////////////////////////////////////////////////
module.exports = router;
