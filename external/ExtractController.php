<?php
use Core\models\Module;
use Core\models\ACL\User;
use Core\models\ACL\TempLogin;
use StormDb\models\Project;
use Core\models\Exception;
use StormDb\models\SerieType;
use StormDb\models\ProjectSerieType;
use StormDb\models\Filter;
/**
* ProjectController
*
* @author
*
* @version
*
*/
require_once 'Zend/Controller/Action.php';
require_once 'StormDb/models/Project.php';

class StormDb_ExtractController extends Zend_Controller_Action
{

   protected function _reportError($error)
   {
       die("error: $error");
   }

   public function init()
   {
       if (! Module::loadModule('StormDb')) {
           die('Module not enabled!');
       }
       if (! $this->getFrontController()->getParam('noViewRenderer')) {
           \Zend_Layout::getMvcInstance()->disableLayout();
           $this->_helper->viewRenderer->setNoRender(true);
       }
       $action = $this->getRequest()->getActionName();
       if (! in_array($action, array(
           'login',
           'modalitytypes'
       ))) {
           if (! User::defaultUser()->isLoggedIn()) {
               try {
                   TempLogin::start('Ptdb'); // could not replace to stormdb, as this would require everyone to change token...
               } catch (Exception $e) {
                   $this->_reportError('Could not login. Wrong URL?');
               }
               $this->templogin = 1;
               $this->tempsession = TempLogin::hasSpecificSession();
           }
           if ($action != 'projects') {
               $projectId = (int) $this->getParam('projectId', 0);
               $project = Project::factoryFromId($projectId);
               if (! $project) {
                   $projectCode = strip_tags(trim($this->getParam('projectCode')));
                   $project = Project::factoryFromCode($projectCode);
               }
               if (! $project) {
                   $this->_reportError("The project does not exist! $projectId $projectCode");
               }
               if (! $project->hasViewPriv()) {
                   $this->_reportError('You are not allowed to view this project!');
               }
               $this->project = $project;
               $this->filterObj = \StormDb\models\Filters::singleton($project);
           }
       }
   }

   public function loginAction()
   {
       $username = strip_tags(trim($this->getParam('username', '')));
       $password = strip_tags(trim($this->getParam('password', '')));
       if (! User::defaultUser()->loginUser($username, $password)) {
           $this->_reportError('Could not login!');
       }
       $url = \Core\models\ACL\TempLogin::genUrl('Ptdb');
       echo "$url[key]=$url[val]";
   }

   public function testloginAction()
   {
       echo 1;
   }

   public function projectsAction()
   {
       $projects = Project::getResults('', 'code');
       foreach ($projects as $project) {
           /* @var $project \StormDb\models\Project */
           if (! $project->hasViewPriv()) {
               continue;
           }
           echo $project->code . "\n";
       }
   }

   public function projecttypesAction()
   {
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       $projectSerieTypes = $project->serieTypes();
       $tmp = [];
       foreach ($projectSerieTypes as $projectSerieType) {
           $tmp[] = $projectSerieType->title();
       }
       echo implode("\n", $tmp);
   }

   /**
    * Returns the subjects in a project
    * The included parameter has the following meaning: 1: only included.
    * -1: only excluded. 0: All
    */
   public function subjectsAction()
   {
       $included = (int) $this->getParam('included', 1);
       $filter = $this->getParam('filter');
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if ($filter) {
           $filterObj = $this->filterObj;
           $filterObj->setUseTempFilters(true);
           $this->_addFilterAndCheckForMeta($project, $filter, $filterObj, 'subject', 'subjectNo');
       }
       $subjects = $project->subjects();
       foreach ($subjects as $s) {
           /*  @var $s \StormDb\models\Subject */
           if ($included == 1 && $s->excluded) {
               continue;
           } elseif ($included == - 1 && ! $s->excluded) {
               continue;
           }
           echo $s->formatId(false) . "\n";
       }
   }

   public function subjectswithcodeAction()
   {
       $filter = $this->getParam('filter');
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if ($filter) {
           $filterObj = $this->filterObj;
           $filterObj->setUseTempFilters(true);
           $this->_addFilterAndCheckForMeta($project, $filter, $filterObj, 'subject', 'subjectNo');
       }

       $subjects = $project->subjects();
       foreach ($subjects as $s) {
           /*  @var $s \StormDb\models\Subject */
           if ($s->excluded) {
               continue;
           }
           echo $s->formatId(true) . "\n";
       }
   }

   public function excludedsubjectswithcodeAction()
   {
       $filter = $this->getParam('filter');
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if ($filter) {
           $filterObj = $this->filterObj;
           $filterObj->setUseTempFilters(true);
           $this->_addFilterAndCheckForMeta($project, $filter, $filterObj, 'subject', 'subjectNo');
       }
       $subjects = $project->subjects();
       foreach ($subjects as $s) {
           /*  @var $s \StormDb\models\Subject */
           if (! $s->excluded) {
               continue;
           }
           echo $s->formatId(true) . "\n";
       }
   }

   public function subjectinfoAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $fields = array(
           'subjectNo',
           'subjectCode',
           'excluded',
           'excludedCategory',
           'excludedReason',
           'comments'
       );
       if (StormDbModule::extractMayUseNames()) {
           $fields[] = 'name';
       }
       $longTextFields = array(
           'excludeReason',
           'comments'
       );
       foreach ($fields as $key) {
           $val = $subject->__get($key);
           if (in_array($key, $longTextFields)) {
               $val = str_replace("\n", '|', trim($val));
           }
           if ($key == 'excluded') {
               $val = (int) $val;
           } elseif ($key == 'excludedCategory') {
               if ($val) {
                   $excludedCategory = \StormDb\models\ExcludedCategory::factoryFromId($val);
                   if (! $excludedCategory) {
                       trigger_error('Missing excluded category');
                   }
                   $val = $excludedCategory->code;
               }
           }
           echo "$key\$" . str_replace(array(
               "\n",
               '$'
           ), '', $val) . "\n";
       }
   }

   public function setsubjectinfofieldsAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       if (! $subjectNo) {
           $this->_reportError('Missing subject number');
       }

       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */

       $infoFields = $subject->setFields();
       foreach ($infoFields as $key => $i) {
           echo "$key\$$i[title]\n";
       }
   }

   public function setsubjectinfoAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $prop = strip_tags(trim($this->getParam('prop')));
       $val = strip_tags(trim($this->getParam('val')));
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if (! $project->isActive()) {
           $this->_reportError('The project is no longer active');
       }

       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       if (strtolower($prop) == 'excludedcategory') {
           if ($val) {
               $excludedCategory = \StormDb\models\ExcludedCategory::factoryFromCode($val);
               if (! $excludedCategory) {
                   $this->_reportError('Illegal excluded category');
               }
               $val = $excludedCategory->id;
           } else {
               $val = null;
           }
       } else {
           $infoFields = $subject->setFields();
           if (! isset($infoFields[$prop])) {
               $this->_reportError('The specified property does not exist');
           }
           $i = $infoFields[$prop];
           if ($i['type'] == 'int') {
               $val = (int) $val;
           } elseif (($i['type'] == 'enum')) {
               $values = $i['values'];
               if (! isset($values[$val])) {
                   $this->_reportError('Illegal value');
               }
           }
       }
       $subject->__set($prop, $val);
       $subject->save();
   }

   /**
    *
    * @return \StormDb\models\Subject
    */
   protected function _subjectFromSubjectNo($subjectNo)
   {
       require_once 'StormDb/models/Subject.php';
       if (! $subjectNo) {
           $this->_reportError('Missing subject number');
       }
       $project = $this->project;
       if (! strpos($subjectNo, '=') && ! strpos($subjectNo, '>') && ! strpos($subjectNo, '<')) {
           $subjects = \StormDb\models\Subject::getResults(array(
               'projectId=?' => $project->id,
               'subjectNo=?' => $subjectNo
           ), '', 1);
       } else {
           $filterObj = $this->filterObj;
           $filterObj->setUseTempFilters(true);

           $this->_addFilterAndCheckForMeta($project, $subjectNo, $filterObj, 'subject', 'subjectNo');
           $subjects = $project->subjects();
       }
       if (! count($subjects)) {
           $this->_reportError('The specified subject does not exist!');
       } elseif (count($subjects) > 1) {
           $this->_reportError('More than one subject matches');
       }
       return current($subjects);
   }

   public function subjectcommentsAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       echo $subject->comments;
   }

   public function subjectagefromcprAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $cpr = $subject->patientId;
       // Check if this is a real CPR or a dummy id
       if (strlen($cpr) == 10 && isInt(substr($cpr, 0, 6))) {
           $day = substr($cpr, 0, 2);
           $month = substr($cpr, 2, 2);
           $year = substr($cpr, 4, 2);
           if ($year > date('y')) {
               $year += 1900;
           } else {
               $year += 2000;
           }
           $timediff = time() - mktime(0, 0, 0, $month, $day, $year);
           echo round($timediff / 3600 / 24 / 365.25, 1);
       }
   }

   public function subjectnumberinotherprojectAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $otherProjectId = (int) $this->getParam('otherProjectId');
       $project = $this->project;
       $otherProject = Project::factoryFromId($otherProjectId);
       if ($otherProject && ! $otherProject->hasViewPriv()) {
           $otherProject = null;
       }
       if (! $otherProject) {
           $projectCode = strip_tags(trim($this->getParam('otherProjectCode')));
           $otherProject = Project::factoryFromCode($projectCode);
       }
       if (! $otherProject) {
           $this->_reportError('The project does not exist! ' . $otherProjectId . ' ' . $projectCode);
       }

       if (! $otherProject->hasViewPriv()) {
           $this->_reportError('You are not allowed to view this project!');
       }
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $whereClause = array(
           'projectId=?' => $otherProject->id,
           'patientId=?' => $subject->patientId
       );
       $otherSubject = \StormDb\models\Subject::getResults($whereClause, '', 1);
       if (count($otherSubject)) {
           echo $otherSubject[0]->subjectNo;
       }
   }

   public function createsubjectAction()
   {
       $subjectNo = $this->getParam('subjectNo', '');
       $subjectId = $this->getParam('subjectId', '');
       $subjectName = $this->getParam('subjectName', '');
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       $subject = $project->createSubject($subjectNo);

       if (! $subjectName) {
           $subjectName = "Subject " . $subject->subjectNo;
       }
       $subject->name = $subjectName;
       $subject->patientId = $subjectId;
       $subject->save();
       echo $subject->formatId(true) . "\n";
   }

   public function studiesAction()
   {
       $subjectNo = $this->getParam('subjectNo');
       $format = $this->getParam('format', 'fs');
       $included = (int) $this->getParam('included', 1);
       $filter = $this->getParam('filter');
       if ($filter) {
           $filterObj = $this->filterObj;
           $filterObj->setUseTempFilters(true);
           $this->_addFilterAndCheckForMeta($this->project, $filter, $filterObj, 'study', 'studyNumber');
       }

       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $studies = $subject->studies();
       $c = 0;
       foreach ($studies as $s) {
           /*  @var $s \StormDb\models\Study */
           ++ $c;
           if ($included == 1 && $s->excluded) {
               continue;
           } elseif ($included == - 1 && ! $s->excluded) {
               continue;
           }
           switch ($format) {
               case 'numeric':
                   echo "$c\n";
                   break;
               default:
                   echo $this->_formatTime($s->studyTime, $format) . "\n";
           }
       }
   }

   protected function _formatTime($time, $format)
   {
       switch ($format) {
           case 'db':
               return $time;
               break;
           case 'fs':
               return str_replace(array(
                   '-',
                   ':',
                   ' '
               ), array(
                   '',
                   '',
                   '_'
               ), $time);
               break;
           default:
               $this->_reportError('Unknown time format');
               break;
       }
   }

   /**
    *
    * @return \StormDb\models\Study
    */
   protected function _studyFromNoOrTime(\StormDb\models\Subject $subject, $study)
   {
       require_once 'StormDb/models/Study.php';
       if (! $study) {
           $this->_reportError('Missing study');
       }
       if (isInt($study)) {
           $studyNo = $study - 1;
           $res = \StormDb\models\Study::getResults('subjectId=' . $subject->id . ' AND (excluded=0 OR excluded IS NULL)', 'studyTime');
           if ($studyNo < 0 || count($res) <= $studyNo) {
               $this->_reportError(' The specified study does not exist! int');
           }
           $study = $res[$studyNo];
       } else {
           // Check if it is a metatag or a date
           if (! strpos($study, '=') && ! strpos($study, '>') && ! strpos($study, '<')) {
               if (strpos($study, '_')) {
                   // Date format may be both Ymd_His or Y-m-d H:i:s
                   $s = substr($study, 0, 4) . '-';
                   $s .= substr($study, 4, 2) . '-' . substr($study, 6, 2) . ' ' . substr($study, 9, 2) . ':' . substr($study, 11, 2) . ':' . substr($study, 13, 2);
                   $study = $s;
               }
               $studies = \StormDb\models\Study::getResults(array(
                   'subjectId=?' => $subject->id,
                   'studyTime=?' => $study
               ), '', 1);
           } else {
               $project = $this->project;
               /* @var $project \StormDb\models\Project */
               $filterObj = $this->filterObj;
               $filterObj->setUseTempFilters(true);
               $this->_addFilterAndCheckForMeta($project, $study, $filterObj, 'study', null);
               $studies = $subject->studies();
           }
           if (! count($studies)) {
               $this->_reportError('The specified study does not exist!');
           } elseif (count($studies) > 1) {
               $this->_reportError('Too many studies matches');
           }
           $study = $studies[0];
       }

       return $study;
   }

   public function studyinfoAction()
   {
       /* @var $project \StormDb\models\Project */
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $project = $this->project;

       $fields = array(
           'studyTime',
           'excluded',
           'excludedCategory',
           'excludedReason',
           'comments'
       );
       if ($project->usesAge) {
           $fields[] = 'age';
       }
       if ($project->usesWeight) {
           $fields[] = 'weight';
       }
       if ($project->usesHeight) {
           $fields[] = 'height';
       }

       $longTextFields = array(
           'excludeReason',
           'comments'
       );
       foreach ($fields as $key) {
           $val = $study->__get($key);
           if (in_array($key, $longTextFields)) {
               $val = str_replace("\n", '|', trim($val));
           }
           if ($key == 'excluded') {
               $val = (int) $val;
           } elseif ($key == 'excludedCategory') {
               if ($val) {
                   $excludedCategory = \StormDb\models\ExcludedCategory::factoryFromId($val);
                   if (! $excludedCategory) {
                       trigger_error('Missing excluded category');
                   }
                   $val = $excludedCategory->code;
               }
           }
           echo "$key\$" . str_replace(array(
               "\n",
               '$'
           ), '', $val) . "\n";
       }
   }

   public function setstudyinfofieldsAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */

       $infoFields = $study->setFields();
       foreach ($infoFields as $key => $i) {
           echo "$key\$$i[title]\n";
       }
   }

   public function setstudyinfoAction()
   {
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if (! $project->isActive()) {
           $this->_reportError('The project is no longer active');
       }
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $prop = strip_tags(trim($this->getParam('prop')));
       $val = strip_tags(trim($this->getParam('val')));
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       if (strtolower($prop) == 'excludedcategory') {
           if ($val) {
               $excludedCategory = \StormDb\models\ExcludedCategory::factoryFromCode($val);
               if (! $excludedCategory) {
                   $this->_reportError('Illegal excluded category');
               }
               $val = $excludedCategory->id;
           } else {
               $val = null;
           }
       } else {
           $infoFields = $study->setFields();
           if (! isset($infoFields[$prop])) {
               $this->_reportError('The specified property does not exist');
           }
           $i = $infoFields[$prop];
           if ($i['type'] == 'int') {
               $val = (int) $val;
           } elseif (($i['type'] == 'enum')) {
               $values = $i['values'];
               if (! isset($values[$val])) {
                   $this->_reportError('Illegal value');
               }
           }
       }
       $study->__set($prop, $val);
       $study->save();
   }

   public function modalitytypesAction()
   {
       $modalityTypes = \StormDb\models\ModalityType::getResults('', 'title');
       foreach ($modalityTypes as $mt) {
           echo $mt->title . "\n";
       }
   }

   public function studycommentsAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       echo $study->comments;
   }

   public function modalitiesAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modalities = $study->modalities();
       foreach ($modalities as $m) {
           /*  @var $m \StormDb\models\Modality */
           echo $m->modalityType()->title . "\n";
       }
   }

   protected function _modalityFromNoOrStr($study, $modality)
   {
       require_once 'StormDb/models/Modality.php';
       if (! $modality) {
           throw new Exception('Missing modality');
       }
       if (isInt($modality)) {
           $modalityNo = $modality - 1;
           $res = \StormDb\models\Modality::getResults('studyId=' . $study->id);
           if ($modalityNo < 0 || count($res) <= $modalityNo) {
               $this->_reportError('The specified modality does not exist!');
           }
           $modality = $res[$modalityNo];
       } else {
           require_once 'StormDb/models/ModalityType.php';
           $modalityType = \StormDb\models\ModalityType::factoryFromTitle($modality);
           if (! $modalityType) {
               $this->_reportError('Wrong modality type!');
           }
           $res = \StormDb\models\Modality::getResults('studyId=' . $study->id . ' AND modalityTypeId=' . $modalityType->id, '', 1);
           if (! count($res)) {
               $this->_reportError('The specified modality does not exist!');
           }
           $modality = $res[0];
       }
       return $modality;
   }

   /**
    * Returns the series for a subject
    * The included parameter has the following meaning: 1: only included.
    * -1: only excluded. 0: All
    */
   public function seriesAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $included = (int) $this->getParam('included', 1);
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $series = $modality->series();
       foreach ($series as $s) {
           /*  @var $s \StormDb\models\Serie */
           if ($included == 1 && $s->excluded) {
               continue;
           } elseif ($included == - 1 && ! $s->excluded) {
               continue;
           }
           echo $s->description . ' ' . $s->serieNo . "\n";
       }
   }

   public function serienumbersAction()
   {
       require_once 'StormDb/models/ProjectSerieType.php';
       require_once 'StormDb/models/Serie.php';
       $project = $this->project;
       /* @var $project \StormDb\models\Project */

       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $seriedescr = strip_tags(trim($this->getParam('seriedescr')));
       $type = strip_tags(trim($this->getParam('type')));
       $included = (int) $this->getParam('included', 1);
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $where = array(
           'modalityId=?' => $modality->id
       );
       $project = $this->project;
       $db = \Core\models\Db\Db::getDb();
       if ($type) {
           // Check for wildcard
           if (strpos($type, '*') !== false) {
               $types = ProjectSerieType::getResults(array(
                   'projectId=?' => $project->id,
                   'title LIKE "?"' => str_replace('*', '%', $type)
               ));
           } else {
               $typeStr = $type;
               $type = ProjectSerieType::factoryFromProjectAndTitle($project, $typeStr);
               if ($type) {
                   $types = array(
                       $type
                   );
               } else {
                   $types = array();
               }
           }
           if (! count($types)) {
               $this->_reportError("unknown type: $typeStr");
           }
           $typeIDs = array();
           foreach ($types as $t) {
               $typeIDs[] = $t->id;
           }
           $where['type IN (?)'] = $typeIDs;
       }
       if ($included == 1) {
           $where[] = '(excluded=0 OR excluded IS NULL)';
       } elseif ($included == - 1) {
           $where[] = 'excluded = 1';
       }
       $series = \StormDb\models\Serie::getResults($where);
       $searchFor = '';
       if ($seriedescr && strpos($seriedescr, '*') !== false) {
           $searchFor = '/' . str_replace(array(
               '.',
               '(',
               ')',
               '*'
           ), array(
               "\\.",
               "\\(",
               "\\)",
               '.*'
           ), $seriedescr) . '/';
       }
       foreach ($series as $s) {
           /*  @var $s \StormDb\models\Serie */
           $found = ! $seriedescr;
           if (! $found) {
               $found = $s->description == $seriedescr;
           }
           if (! $found && $searchFor) {
               $found = preg_match($searchFor, $s->description);
           }
           if ($found) {
               echo $s->serieNo . "\n";
           }
       }
   }

   protected function _addFilterAndCheckForMeta(Project $project, $input, \StormDb\models\Filters $filterObj, $level, $field)
   {
       $code = null;
       if (is_array($input)) {
           $code = $input['code'];
           $comp = $input['comp'];
           $val = $input['val'];
       } elseif (strpos($input, '=') || strpos($input, '>') || strpos($input, '<')) {
           $matches = array();
           if (! preg_match('/([^!=<>]+)([!=<>]+)(.+)/i', $input, $matches)) {
               $this->_reportError("wrong format of study $input");
           }
           $code = trim($matches[1]);
           $comp = trim($matches[2]);
           $val = trim($matches[3]);
       }
       $cf = $filterObj->getCombinedFilter($level);
       if (! is_null($code)) {
           $fields = $cf->getFilterFields(false);
           if (isset($fields[$code])) {
               if (strpos($val, '*') !== false) {
                   $val = str_replace('*', '%', $val);
                   if ($comp == '=') {
                       $comp = 'LIKE';
                   } elseif ($comp == '!=') {
                       $comp = 'NOT LIKE';
                   }
               }
               $cf->createFilter($field, $comp, $val);
           } else {
               $meta = \StormDb\models\MetaType::factoryFromCode($project->id, $code, $level);
               if (! $meta) {
                   $this->_reportError("unknown $level meta $code");
               }
               $cf->createFilter('metatype_' . $meta->code, $comp, $val);
           }
       } else {
           if (is_null($field)) {
               $this->_reportError('not a field!');
           }
           $comp = '=';
           if (strpos($input, '*') !== false) {
               $input = str_replace('*', '%', $input);
               $comp = 'LIKE';
           }
           $cf->createFilter($field, $comp, $input);
       }
   }

   public function filteredseriesAction()
   {
       $subjects = $this->getParam('subjects', '');
       $subjectMetas = $this->getParam('subjectmetas', array());
       $studies = $this->getParam('studies', '');
       $studyMetas = $this->getParam('studymetas', array());
       $modalities = $this->getParam('modalities', '');
       $types = $this->getParam('types', '');
       $anyWithType = $this->getParam('anyWithType', 0);
       $description = $this->getParam('description', '');
       $serieMetas = $this->getParam('seriemetas', array());
       $excluded = $this->getParam('excluded', 0);
       $outputOptions = $this->getParam('outputoptions', array());
       $removeProjects = $this->getParam('removeProjects', '0');

       $project = $this->project;
       $db = \Core\models\Db\Db::getDb();
       $filterObj = $this->filterObj;
       $filterObj->setUseTempFilters(true);
       // remove filters on studies, modalities and series as these will screw up when combined with study numbers
       // $filterObj->clearFilters();
       // SUBJECTS
       // Deal with subject number
       if ($subjects) {
           $this->_addFilterAndCheckForMeta($project, $subjects, $filterObj, 'subject', 'subjectNo');
       } elseif (! $filterObj->getCombinedFilter('subject')->hasFilterField('excluded')) {
           $filterObj->getCombinedFilter('subject')->createFilter('excluded', '=', $excluded);
       }
       // subject metas
       foreach ($subjectMetas as $code => $s) {
           $sArray = explode('$', $s);
           $comp = $sArray[0];
           $val = $sArray[1];
           $input = array(
               'code' => $code,
               'comp' => $comp,
               'val' => $val
           );
           $this->_addFilterAndCheckForMeta($project, $input, $filterObj, 'subject', null);
       }
       // STUDIES
       if (! $filterObj->getCombinedFilter('study')->hasFilterField('excluded')) {
           $filterObj->getCombinedFilter('study')->createFilter('excluded', '=', $excluded);
       }
       if ($studies) {
           // Check if it is a specific date
           if (strpos($studies, '_')) {
               $d = DateTime::createFromFormat(' Ymd_His', $studies);
               if (! $d) {
                   $this->_reportError('The time format is not right');
               }
               $this->_addFilterAndCheckForMeta($project, $d->format('Y-m-d H:i:s'), $filterObj, 'study', 'studyTime');
           } else {
               $this->_addFilterAndCheckForMeta($project, $studies, $filterObj, 'study', 'studyNumber');
           }
       }
       foreach ($studyMetas as $code => $s) {
           $sArray = explode('$', $s);
           $comp = $sArray[0];
           $val = $sArray[1];
           $input = array(
               'code' => $code,
               'comp' => $comp,
               'val' => $val
           );
           $this->_addFilterAndCheckForMeta($project, $input, $filterObj, 'study', null);
       }

       // MODALITIES
       if ($modalities) {
           $tmp = explode('|', $modalities);
           $tmp2 = array();
           foreach ($tmp as $t) {
               $tmp2[] = $db->quote($t);
           }
           $sql = 'SELECT id FROM stormdbModalityTypes WHERE title IN (' . implode(', ', $tmp2) . ')';
           $modalities = $db->fetchCol($sql);
           if (! count($modalities)) {
               return;
           }
           $filterObj->getCombinedFilter('modality')->createFilter('modalityTypeId', '=', $modalities);
       }

       // SERIES
       if (! $filterObj->getCombinedFilter('serie')->hasFilterField('excluded')) {
           $filterObj->getCombinedFilter('serie')->createFilter('excluded', '=', $excluded);
       }
       if ($types) {
           if (strpos($types, '=') || strpos($types, '>') || strpos($types, '<')) {
               $this->_addFilterAndCheckForMeta($project, $types, $filterObj, 'serie', null);
           } else {
               $tmp = explode('|', $types);
               $tmp2 = array();
               foreach ($tmp as $t) {
                   $t = str_replace('*', '%', $t);
                   $tmp2[] = $db->quoteInto('TITLE like (?)', $t);
               }
               $sql = 'SELECT id FROM stormdbProjectSerieTypes WHERE (' . implode(' OR ', $tmp2) . ')';

               $types = $db->fetchCol($sql);
               if (! count($types)) {
                   return;
               }
               $filterObj->getCombinedFilter('serie')->createFilter('type', '=', $types);
           }
       } elseif ($anyWithType) {
           $filterObj->getCombinedFilter('serie')->createFilter('type', '!=', 0);
       }
       if ($description) {
           $this->_addFilterAndCheckForMeta($project, $description, $filterObj, 'serie', 'description');
       }
       foreach ($serieMetas as $code => $s) {
           $sArray = explode('$', $s);
           $comp = $sArray[0];
           $val = $sArray[1];
           $input = array(
               'code' => $code,
               'comp' => $comp,
               'val' => $val
           );
           $this->_addFilterAndCheckForMeta($project, $input, $filterObj, 'serie', null);
       }

       echo $this->_allseriesinfilter($project, $outputOptions, $excluded, $removeProjects);
   }

   protected function _allseriesinfilter(Project $project, $outputOptions, $excluded, $removeProjects)
   {
       $formatMetas = false;
       // output options
       $studyTimeFormat = isset($outputOptions['studyformat']) ? $outputOptions['studyformat'] : 'fs';
       $inclPath = isset($outputOptions['inclpath']) ? $outputOptions['inclpath'] : true;
       $inclFiles = isset($outputOptions['inclfiles']) ? $outputOptions['inclfiles'] : false;
       // subjectmetas
       $subjectMetas = array();
       if (isset($outputOptions['subjectmetas'])) {
           $subjectMetasCodes = explode('|', $outputOptions['subjectmetas']);
           foreach ($subjectMetasCodes as $prop) {
               $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_SUBJECT, $prop);
               if (! $metaType) {
                   $this->_reportError('wrong property name!');
               }
               $subjectMetas[] = $metaType;
           }
       }
       // studyMetas
       $studyMetas = array();
       if (isset($outputOptions['studymetas'])) {
           $studyMetasCodes = explode('|', $outputOptions['studymetas']);
           foreach ($studyMetasCodes as $prop) {
               $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_STUDY, $prop);
               if (! $metaType) {
                   die('error: wrong property name!');
               }
               $studyMetas[] = $metaType;
           }
       }
       $serieFields = array();
       if (isset($outputOptions['seriefields'])) {
           $serieFields = explode('|', $outputOptions['seriefields']);
       }
       // serieMetas
       $serieMetas = array();
       if (isset($outputOptions['seriemetas'])) {
           $serieMetasCodes = explode('|', $outputOptions['seriemetas']);
           foreach ($serieMetasCodes as $prop) {
               $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_SERIE, $prop);
               if (! $metaType) {
                   $this->_reportError('wrong property name!');
               }
               $serieMetas[] = $metaType;
           }
       }

       $subjects = $project->subjects();
       $exclusionCategories = \StormDb\models\ExcludedCategory::getExclusionCategories(true);
       foreach ($subjects as $su) {
           /* @var $su \StormDb\models\Subject */
           $studies = $su->studies();
           $subjectRow = array(
               'subject' => $su->subjectNo,
               'subjectcode' => $su->formatId(true)
           );
           foreach ($subjectMetas as $sm) {
               $res = $su->metaVal($metaType, $formatMetas);
               $subjectRow[$sm->code] = is_null($res) ? 'NULL' : $res;
           }

           foreach ($studies as $st) {
               /* @var $st \StormDb\models\Study */
               $studyRow = array(
                   'study' => $this->_formatTime($st->studyTime, $studyTimeFormat)
               );
               if ($project->usesAge) {
                   $studyRow['age'] = $st->age;
               }
               if ($project->usesWeight) {
                   $studyRow['weight'] = $st->weight;
               }
               if ($project->usesHeight) {
                   $studyRow['height'] = $st->height;
               }

               foreach ($studyMetas as $sm) {
                   $res = $st->metaVal($metaType, $formatMetas);
                   $studyRow[$sm->code] = is_null($res) ? 'NULL' : $res;
               }
               $modalities = $st->modalities();
               foreach ($modalities as $moType => $mo) {
                   /* @var $m \StormDb\models\Modality */
                   $series = $mo->series();
                   foreach ($series as $se) {
                       /* @var $se \StormDb\models\Serie */
                       $row = $subjectRow + $studyRow + array(
                           'modality' => $moType,
                           'serieno' => $se->serieNo,
                           'type' => $se->projectSerieType() ? $se->projectSerieType()->title() : '',
                           'qscore' => $se->qscore,
                           'serieDbId' => $se->id
                       );
                       if ($inclPath || $inclFiles) {
                           $row['path'] = $se->pathWithFiles($removeProjects);
                       }
                       if ($inclFiles) {
                           $row['files'] = implode('|', $se->files());
                       }
                       if ($excluded) {
                           $row['subjectexclusioncategory'] = $su->excludedCategory ? $exclusionCategories[excludedCategory] : '';
                           $row['studyexclusioncategory'] = $st->excludedCategory ? $exclusionCategories[$st->excludedCategory] : '';
                           $row['serieexclusioncategory'] = $se->excludedCategory ? $exclusionCategories[$se->excludedCategory] : '';
                       }
                       foreach ($serieMetas as $sm) {
                           $res = $se->metaVal($metaType, $formatMetas);
                           $row[$sm->code] = is_null($res) ? 'NULL' : $res;
                       }
                       foreach ($serieFields as $sf) {
                           $row[$sf] = $se->__get($sf);
                       }
                       array_walk($row, create_function('&$i, $k', '$i="$k:$i";'));
                       echo implode("\$", $row) . "\n";
                   }
               }
           }
       }
   }

   public function filteredmodalitiesAction()
   {
       $subjects = $this->getParam('subjects', '');
       $subjectMetas = $this->getParam('subjectmetas', array());
       $studies = $this->getParam('studies', '');
       $studyMetas = $this->getParam('studymetas', array());
       $modalities = $this->getParam('modalities', '');
       $excluded = $this->getParam('excluded', 0);
       $outputOptions = $this->getParam('outputoptions', array());

       $project = $this->project;
       $db = \Core\models\Db\Db::getDb();
       $filterObj = $this->filterObj;
       $filterObj->setUseTempFilters(true);
       // remove filters on studies, modalities and series as these will screw up when combined with study numbers
       // $filterObj->clearFilters();
       // SUBJECTS
       // Deal with subject number
       if ($subjects) {
           $this->_addFilterAndCheckForMeta($project, $subjects, $filterObj, 'subject', 'subjectNo');
       } elseif (! $filterObj->getCombinedFilter('subject')->hasFilterField('excluded')) {
           $filterObj->getCombinedFilter('subject')->createFilter('excluded', '=', $excluded);
       }
       // subject metas
       foreach ($subjectMetas as $code => $s) {
           $sArray = explode('$', $s);
           $comp = $sArray[0];
           $val = $sArray[1];
           $input = array(
               'code' => $code,
               'comp' => $comp,
               'val' => $val
           );
           $this->_addFilterAndCheckForMeta($project, $input, $filterObj, 'subject', null);
       }
       // STUDIES
       if (! $filterObj->getCombinedFilter('study')->hasFilterField('excluded')) {
           $filterObj->getCombinedFilter('study')->createFilter('excluded', '=', $excluded);
       }
       if ($studies) {
           $this->_addFilterAndCheckForMeta($project, $studies, $filterObj, 'study', 'studyNumber');
       }
       foreach ($studyMetas as $code => $s) {
           $sArray = explode('$', $s);
           $comp = $sArray[0];
           $val = $sArray[1];
           $input = array(
               'code' => $code,
               'comp' => $comp,
               'val' => $val
           );
           $this->_addFilterAndCheckForMeta($project, $input, $filterObj, 'study', null);
       }

       // MODALITIES
       if ($modalities) {
           $tmp = explode('|', $modalities);
           $tmp2 = array();
           foreach ($tmp as $t) {
               $tmp2[] = $db->quote($t);
           }
           $sql = 'SELECT id FROM stormdbModalityTypes WHERE title IN (' . implode(', ', $tmp2) . ')';
           $modalities = $db->fetchCol($sql);
           $filterObj->getCombinedFilter('subject')->createFilter('modalityTypeId', '=', $modalities);
       }

       echo $this->_allmodalitiesinfilter($project, $outputOptions, $excluded);
   }

   protected function _allmodalitiesinfilter(Project $project, $outputOptions, $excluded)
   {
       $formatMetas = false;
       // output options
       $studyTimeFormat = isset($outputOptions['studyformat']) ? $outputOptions['studyformat'] : 'fs';
       $inclPath = isset($outputOptions['inclpath']) ? $outputOptions['inclpath'] : true;
       $inclFiles = isset($outputOptions['inclfiles']) ? $outputOptions['inclfiles'] : false;

       // subjectmetas
       $subjectMetas = array();
       if (isset($outputOptions['subjectmetas'])) {
           $subjectMetasCodes = explode('|', $outputOptions['subjectmetas']);
           foreach ($subjectMetasCodes as $prop) {
               $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_SUBJECT, $prop);
               if (! $metaType) {
                   $this->_reportError('wrong property name!');
               }
               $subjectMetas[] = $metaType;
           }
       }
       // studyMetas
       $studyMetas = array();
       if (isset($outputOptions['studymetas'])) {
           $studyMetasCodes = explode('|', $outputOptions['studymetas']);
           foreach ($studyMetasCodes as $prop) {
               $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_STUDY, $prop);
               if (! $metaType) {
                   $this->_reportError('wrong property name!');
               }
               $studyMetas[] = $metaType;
           }
       }

       $subjects = $project->subjects();
       $exclusionCategories = \StormDb\models\ExcludedCategory::getExclusionCategories(true);
       foreach ($subjects as $su) {
           /* @var $su \StormDb\models\Subject */
           $studies = $su->studies();
           $subjectRow = array(
               'subject' => $su->subjectNo
           );
           foreach ($subjectMetas as $sm) {
               $res = $su->metaVal($metaType, $formatMetas);
               $subjectRow[$sm->code] = is_null($res) ? 'NULL' : $res;
           }

           foreach ($studies as $st) {
               /* @var $st \StormDb\models\Study */
               $studyRow = array(
                   'study' => $this->_formatTime($st->studyTime, $studyTimeFormat)
               );
               foreach ($studyMetas as $sm) {
                   $res = $st->metaVal($metaType, $formatMetas);
                   $studyRow[$sm->code] = is_null($res) ? 'NULL' : $res;
               }
               $modalities = $st->modalities();
               foreach ($modalities as $moType => $mo) {
                   /* @var $m \StormDb\models\Modality */
                   $row = $subjectRow + $studyRow + array(
                       'modality' => $moType
                   );
                   if ($excluded) {
                       $row['subjectexclusioncategory'] = $su->excludedCategory ? $exclusionCategories[excludedCategory] : '';
                       $row['studyexclusioncategory'] = $st->excludedCategory ? $exclusionCategories[$st->excludedCategory] : '';
                   }
                   array_walk($row, create_function('&$i, $k', '$i="$k:$i";'));
                   echo implode("\$", $row) . "\n";
               }
           }
       }
   }

   /**
    * Finds the serie from modality and serie number.
    * First tries to find the serie with a set serie type
    *
    * @param \StormDb\models\Modality $modality            
    * @param int $serieNo            
    * @param boolean $onlyIncluded            
    * @return \StormDb\models\Serie
    */
   protected function _serieFromNo($modality, $serieNo, $onlyIncluded = true)
   {
       require_once 'StormDb/models/Serie.php';
       $filter = 'modalityId=' . $modality->id . ' AND serieNo=' . (int) $serieNo;
       if ($onlyIncluded) {
           $filter .= " AND (excluded=0 OR excluded IS NULL)";
       }
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if ($project->hasSerieTypes()) {
           $res = \StormDb\models\Serie::getResults("$filter AND type IS NOT NULL");
           if (! count($res)) {
               $res = \StormDb\models\Serie::getResults($filter);
           }
       } else {
           $res = \StormDb\models\Serie::getResults($filter);
       }
       if (! count($res)) {
           $this->_reportError('The specified serie does not exist');
       }
       if (count($res) > 1) {
           $this->_reportError('More than one serie with same serie number');
       }
       return $res[0];
   }

   /**
    * Finds the serie from modality and serie number.
    * First tries to find the serie with a set serie type
    *
    * @param \StormDb\models\Modality $modality            
    * @param string $serieType            
    * @param boolean $onlyIncluded            
    * @return \StormDb\models\Serie
    */
   protected function _serieFromType($modality, $serieType, $onlyIncluded = true)
   {
       require_once 'StormDb/models/Serie.php';
       $project = $this->project;
       $projectSerieTypeObj = ProjectSerieType::factoryFromProjectAndTitle($project, $serieType);
       if (! $projectSerieTypeObj) {
           $this->_reportError('The specified type does not exist');
       }
       $filter = 'modalityId=' . $modality->id . ' AND type=' . (int) $projectSerieTypeObj->id;
       if ($onlyIncluded) {
           $filter .= " AND (excluded=0 OR excluded IS NULL)";
       }
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       $res = \StormDb\models\Serie::getResults($filter);
       if (! count($res)) {
           $this->_reportError('The specified serie does not exist ' . $projectSerieTypeObj->id);
       }
       if (count($res) > 1) {
           $this->_reportError('More than one serie with same serie number');
       }
       return $res[0];
   }

   /**
    *
    * @return \StormDb\models\Auxserie
    */
   protected function _auxSerieFromSerie($serie, $aux)
   {
       if (isInt($aux)) {
           $auxNo = $aux - 1;
           $res = \StormDb\models\Auxserie::getResults('serieId=' . $serie->id);
           if ($auxNo < 0 || count($res) <= $auxNo) {
               $aux = null;
           } else {
               $aux = $res[$auxNo];
           }
       } else {
           $modalityType = \StormDb\models\ModalityType::factoryFromTitle($aux);
           if (! $modalityType) {
               $this->_reportError('Wrong aux serie type!');
           }
           $res = \StormDb\models\Auxserie::getResults('serieId=' . $serie->id . ' AND modalityTypeId=' . $modalityType->id, '', 1);
           if (! count($res)) {
               $aux = null;
           } else {
               $aux = $res[0];
           }
       }
       return $aux;
   }

   public function filesAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $removeProjects = $this->getParam('removeProjects', '0');
       $serieNo = $this->getParam('serieNo');
       $serieType = $this->getParam('serieType');
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = null;
       if ($serieNo) {
           $serie = $this->_serieFromNo($modality, $serieNo);
       } elseif ($serieType) {
           $serie = $this->_serieFromType($modality, $serieType);
       }
       if (! $serie) {
           $this->_reportError('The specified serie does not exist');
       }

       /* @var $serie \StormDb\models\Serie */
       $files = $serie->files();
       $path = $serie->pathWithFiles($removeProjects);
       foreach ($files as $f) {
           echo "$path/$f\n";
       }
   }

   public function serieinfoAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $serieNo = $this->getParam('serieNo');
       $onlyIncluded = (int) $this->getParam('onlyIncluded', 1);
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = $this->_serieFromNo($modality, $serieNo, $onlyIncluded);
       /* @var $serie \StormDb\models\Serie */
       $data = $serie->toArray();
       $skipFields = array(
           'id',
           'modalityId',
           'subjectId',
           'infoAdded',
           'mailsent'
       );
       $longTextFields = array(
           'excludeReason',
           'comments'
       );
       $out = array();
       foreach ($data as $key => $val) {
           if (in_array($key, $skipFields)) {
               continue;
           }
           if (in_array($key, $longTextFields)) {
               $val = str_replace("\n", '|', trim($val));
           }
           if ($key == 'type' && $val) {
               $type = $serie->projectSerieType();
               if (! $type) {
                   trigger_error('MIssing type!');
               }
               $val = $type->title();
           } elseif ($key == 'excluded') {
               $val = (int) $val;
           } elseif ($key == 'excludedCategory') {
               if ($val) {
                   $excludedCategory = \StormDb\models\ExcludedCategory::factoryFromId($val);
                   if (! $excludedCategory) {
                       trigger_error('Missing excluded category');
                   }
                   $val = $excludedCategory->code;
               }
           }
           $out[$key] = $val;
       }
       if ($this->project->usesAge) {
           $out['age'] = $study->age;
       }
       $out['studyTime'] = $study->studyTime;
       $out['modality'] = $modality->modalityType()->title;
       foreach ($out as $key => $val) {
           echo "$key\$" . str_replace(array(
               "\n",
               '$'
           ), '', $val) . "\n";
       }
   }

   public function setserieinfofieldsAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));

       $serieNo = $this->getParam('serieNo');
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = $this->_serieFromNo($modality, $serieNo);
       /* @var $serie \StormDb\models\Serie */

       $infoFields = $serie->setFields();
       foreach ($infoFields as $key => $i) {
           echo "$key\$$i[title]\n";
       }
   }

   public function setserieinfoAction()
   {
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if (! $project->isActive()) {
           $this->_reportError('The project is no longer active');
       }
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $prop = strip_tags(trim($this->getParam('prop')));
       $val = strip_tags(trim($this->getParam('val')));

       $serieNo = $this->getParam('serieNo');
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = $this->_serieFromNo($modality, $serieNo, false);
       /* @var $serie \StormDb\models\Serie */
       $db = \Core\models\Db\Db::getDb();

       $project = $this->project;

       // Special handlings first
       if (strtolower($prop) == 'type') {
           if ($val) {
               $projectSerieType = ProjectSerieType::factoryFromProjectAndTitle($project, $val);
               if ($projectSerieType) {
                   $val = $projectSerieType->id;
               } else {
                   $this->_reportError('Illegal type');
               }
           } else {
               $val = null;
           }
       } elseif (strtolower($prop) == 'excludedcategory') {
           if ($val) {
               $excludedCategory = \StormDb\models\ExcludedCategory::factoryFromCode($val);
               if (! $excludedCategory) {
                   die('error: Illegal excluded category');
               }
               $val = $excludedCategory->id;
           } else {
               $val = null;
           }
       } else {
           $infoFields = $serie->setFields();
           if (! isset($infoFields[$prop])) {
               $this->_reportError('The specified property does not exist');
           }
           $i = $infoFields[$prop];
           if ($i['type'] == 'int') {
               $val = (int) $val;
           } elseif (($i['type'] == 'enum')) {
               $values = $i['values'];
               if (! isset($values[$val])) {
                   $this->_reportError('Illegal value');
               }
           }
       }
       $serie->__set($prop, $val);
       $serie->save();
   }

   public function setserieexcludedAction()
   {
       // TODO Should this be allowed after inactivation?
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $serieNo = (int) $this->getParam('serieNo');
       $seriedescr = strip_tags(trim($this->getParam('seriedescr')));
       $exclude = (int) $this->getParam('exclude');
       if (! $serieNo && (! $seriedescr || ! trim(str_replace('*', '', $seriedescr)))) {
           $this->_reportError('Filter is too loose');
       }
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $where = array(
           'modalityId=?' => $modality->id
       );
       if ($serieNo) {
           $where['serieNo=?'] = $serieNo;
       }
       $series = \StormDb\models\Serie::getResults($where);
       $searchFor = '';
       if ($seriedescr && strpos($seriedescr, '*') !== false) {
           $searchFor = '/' . str_replace(array(
               '.',
               '(',
               ')',
               '*'
           ), array(
               "\\.",
               "\\(",
               "\\)",
               '.*'
           ), $seriedescr) . '/';
       }
       foreach ($series as $s) {
           /*  @var $s \StormDb\models\Serie */
           $found = ! $seriedescr;
           if (! $found) {
               $found = $s->description == $seriedescr;
           }
           if (! $found && $searchFor) {
               $found = preg_match($searchFor, $s->description);
           }
           if ($found) {
               $s->excluded = $exclude;
               $s->save();
           }
       }
   }

   public function seriecommentsAction()
   {
       /* @var $project \StormDb\models\Project */
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $removeProjects = $this->getParam('removeProjects', '0');
       $serieNo = $this->getParam('serieNo');
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = $this->_serieFromNo($modality, $serieNo);
       /* @var $serie \StormDb\models\Serie */
       echo $serie->comments;
   }

   public function auxseriesAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $serieNo = (int) $this->getParam('serieNo');
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = $this->_serieFromNo($modality, $serieNo);
       /* @var $serie \StormDb\models\Serie */
       $auxseries = $serie->auxseries();
       foreach ($auxseries as $auxserie) {
           /* @var $auxserie \StormDb\models\Auxserie */
           echo $auxserie->modalityType()->title . "\n";
       }
   }

   public function auxfilesAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $serieNo = (int) $this->getParam('serieNo');
       $aux = strip_tags(trim($this->getParam('aux')));
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = $this->_serieFromNo($modality, $serieNo);
       /* @var $serie \StormDb\models\Serie */
       $auxSerie = $this->_auxSerieFromSerie($serie, $aux);
       if ($auxSerie) {
           $path = $auxSerie->path();
           if (! is_dir($path)) {
               $this->_reportError('The specified aux serie type does not exist!');
           }
           $files = $auxSerie->files();
           foreach ($files as $f) {
               echo "$path/$f\n";
           }
       }
   }

   public function auxseriecommentsAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $serieNo = (int) $this->getParam('serieNo');
       $aux = strip_tags(trim($this->getParam('aux')));
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = $this->_serieFromNo($modality, $serieNo);
       /* @var $serie \StormDb\models\Serie */
       $auxSerie = $this->_auxSerieFromSerie($serie, $aux);
       echo $auxSerie->comments;
   }

   public function subjectmetasAction()
   {
       echo "sex\n";
       $metaTypes = $this->project->metaTypes(\StormDb\models\MetaType::TYPE_SUBJECT, true);
       foreach ($metaTypes as $metaType) {
           echo $metaType->code . "\n";
       }
   }

   public function subjectmetaAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $prop = strip_tags(trim($this->getParam('prop')));
       $project = $this->project;
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       $format = (int) $this->getParam('format');
       if (in_array($prop, array(
           'sex'
       ))) {
           echo $subject->__get($prop);
           return;
       }
       $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_SUBJECT, $prop);
       if (! $metaType) {
           $this->_reportError('wrong property name!');
       }
       if ($metaType->isSensitive && ! $project->displaySensitiveSubjectInfo()) {
           $project->setDisplaySensitiveSubjectInfo(true);
       }
       $res = $subject->metaVal($metaType, $format);
       if (is_null($res)) {
           echo 'NULL';
       } else {
           echo $res;
       }
   }

   public function setsubjectmetaAction()
   {
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if (! $project->isActive()) {
           $this->_reportError('The project is no longer active');
       }

       $subjectNo = (int) $this->getParam('subjectNo');
       $prop = strip_tags(trim($this->getParam('prop')));
       $val = strip_tags(trim($this->getParam('val')));
       $project = $this->project;
       if (! $project->hasEditPriv()) {
           $this->_reportError('You are not allowed to set meta values!');
       }
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       if (in_array($prop, array(
           'sex',
           'height'
       ))) {
           $subject->__set($prop, $val);
           $subject->save();
           return;
       }
       $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_SUBJECT, $prop);
       if (! $metaType) {
           $this->_reportError('wrong property name!');
       }
       if ($metaType->isSensitive && ! $project->displaySensitiveSubjectInfo()) {
           $project->setDisplaySensitiveSubjectInfo(true);
       }

       $out = $subject->setMetaVal($metaType, $val);
       if ($out) {
           $this->_reportError($out);
       }
   }

   public function studymetasAction()
   {
       $metaTypes = $this->project->metaTypes(\StormDb\models\MetaType::TYPE_STUDY, true);
       foreach ($metaTypes as $metaType) {
           echo $metaType->code . "\n";
       }
   }

   public function studymetaAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $prop = strip_tags(trim($this->getParam('prop')));
       $project = $this->project;
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       $study = $this->_studyFromNoOrTime($subject, $study);
       $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_STUDY, $prop);
       if (! $metaType) {
           $this->_reportError('wrong property name!');
       }
       if ($metaType->isSensitive && ! $project->displaySensitiveSubjectInfo()) {
           $project->setDisplaySensitiveSubjectInfo(true);
       }

       $res = $study->metaVal($metaType, false);
       if (is_null($res)) {
           echo 'NULL';
       } else {
           echo $res;
       }
   }

   public function setstudymetaAction()
   {
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if (! $project->isActive()) {
           $this->_reportError('The project is no longer active');
       }

       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $prop = strip_tags(trim($this->getParam('prop')));
       $val = strip_tags(trim($this->getParam('val')));
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       $study = $this->_studyFromNoOrTime($subject, $study);
       $project = $this->project;
       if (! $project->hasEditPriv()) {
           $this->_reportError('You are not allowed to set meta values!');
       }
       $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_STUDY, $prop);
       if (! $metaType) {
           $this->_reportError('wrong property name!');
       }
       if ($metaType->isSensitive && ! $project->displaySensitiveSubjectInfo()) {
           $project->setDisplaySensitiveSubjectInfo(true);
       }

       $out = $study->setMetaVal($metaType, $val);
       if ($out) {
           $this->_reportError($out);
       }
   }

   public function seriemetasAction()
   {
       $metaTypes = $this->project->metaTypes(\StormDb\models\MetaType::TYPE_SERIE, true);
       foreach ($metaTypes as $metaType) {
           echo $metaType->code . "\n";
       }
   }

   public function seriemetaAction()
   {
       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $serieNo = (int) $this->getParam('serieNo');
       $prop = strip_tags(trim($this->getParam('prop')));

       $project = $this->project;
       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = $this->_serieFromNo($modality, $serieNo);
       /* @var $serie \StormDb\models\Serie */
       $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_SERIE, $prop);
       if (! $metaType) {
           $this->_reportError('wrong property name!');
       }
       if ($metaType->isSensitive && ! $project->displaySensitiveSubjectInfo()) {
           $project->setDisplaySensitiveSubjectInfo(true);
       }

       $res = $serie->metaVal($metaType, false);
       if (is_null($res)) {
           echo 'NULL';
       } else {
           echo $res;
       }
   }

   public function setseriemetaAction()
   {
       $project = $this->project;
       /* @var $project \StormDb\models\Project */
       if (! $project->isActive()) {
           $this->_reportError('The project is no longer active');
       }

       $subjectNo = (int) $this->getParam('subjectNo');
       $study = strip_tags(trim($this->getParam('study')));
       $modality = strip_tags(trim($this->getParam('modality')));
       $serieNo = (int) $this->getParam('serieNo');
       $prop = strip_tags(trim($this->getParam('prop')));
       $val = strip_tags(trim($this->getParam('val')));

       $subject = $this->_subjectFromSubjectNo($subjectNo);
       /* @var $subject \StormDb\models\Subject */
       $study = $this->_studyFromNoOrTime($subject, $study);
       /* @var $study \StormDb\models\Study */
       $modality = $this->_modalityFromNoOrStr($study, $modality);
       /* @var $modality \StormDb\models\Modality */
       $serie = $this->_serieFromNo($modality, $serieNo);
       /* @var $serie \StormDb\models\Serie */
       $project = $this->project;
       if (! $project->hasEditPriv()) {
           $this->_reportError('You are not allowed to set meta values!');
       }
       $metaType = $project->metaType(\StormDb\models\MetaType::TYPE_SERIE, $prop);
       if (! $metaType) {
           $this->_reportError('wrong property name!');
       }
       if ($metaType->isSensitive && ! $project->displaySensitiveSubjectInfo()) {
           $project->setDisplaySensitiveSubjectInfo(true);
       }

       $out = $serie->setMetaVal($metaType, $val);
       if ($out) {
           $this->_reportError($out);
       }
   }

   public function addfilterAction()
   {
       $field = $this->getParam('field');
       $comp = $this->getParam('comp');
       $val = $this->getParam('val');
       $type = $this->getParam('type');
       $project = $this->project;
       if ($this->templogin && ! $this->tempsession) {
           $this->_reportError('You have to use a session for filters');
       }
       $filterObj = \StormDb\models\Filters::singleton($project);
       $filterObj->getCombinedFilter($type)->createFilter($field, $comp, $val);
   }

   public function getfilterAction()
   {
       if ($this->templogin && ! $this->tempsession) {
           $this->_reportError('You have to use a session for filters');
       }
       $project = $this->project;
       $filterObj = \StormDb\models\Filters::singleton($project);
       $filter = $filterObj->getFilters();
       foreach ($filter as $type => $combinedFilter) {
           /* @var $combinedFilter \StormDb\models\CombinedFilter */
           $filters = $combinedFilter->getFilters();
           foreach ($filters as $f) {
               /* @var $f \StormDb\models\Filter */
               $field = str_replace("'", "''", $f->getField());
               $val = str_replace("'", "''", $f->getVal());
               $comp = $f->getComp();
               echo "stormdb_add_filter(dbhandle, project_id, '$type', '$field', '$comp', '$val');\n";
           }
       }
   }

   public function clearfilterAction()
   {
       if ($this->templogin && ! $this->tempsession) {
           $this->_reportError('You have to use a session for filters');
       }
       $project = $this->project;
       $filterObj = \StormDb\models\Filters::singleton($project);
       $filterObj->clearCombinedFilters();
   }
}