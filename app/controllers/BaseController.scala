package controllers

import db_controller.DbController._
import models.Person
import javax.inject._
import play.api.libs.json._
import play.api.mvc.{Action, AnyContent, BaseController, ControllerComponents}

@Singleton
class TodoListController @Inject()(val controllerComponents: ControllerComponents)
  extends BaseController {

  implicit val newPersonJson = Json.format[Person]

  def getAll(): Action[AnyContent] = Action {
    val persons = getAllPersonsFromDb()
    if (persons.isEmpty) {
      NoContent
    } else {
      Ok(persons)
    }
  }

  def getPersonById(personId: Integer): Action[AnyContent] = Action {
    val persons = getPersonByIdFromDb(personId)
    if (persons.isEmpty) {
      NoContent
    } else {
      Ok(persons)
    }
  }

  def getAllWorkPlaces(): Action[AnyContent] = Action {
    val workPlaces = getAllWorkPlacesFromDb()
    if (workPlaces.isEmpty) {
      NoContent
    } else {
      Ok(workPlaces)
    }
  }

  def getAllData(): Action[AnyContent] = Action {
    val allData = getAllDataFromDb()
    if (allData.isEmpty) {
      NoContent
    } else {
      Ok(allData)
    }
  }

  def addNewItem() = Action { implicit request =>

    val content = request.body
    val jsonObject = content.asJson

    val newPersonData: Option[Person] = jsonObject.flatMap(Json.fromJson[Person](_).asOpt)

    newPersonData match {
      case Some(newItem) =>
        addNewPerson(newItem.name)
        Created("Json.toJson(toBeAdded)")
      case None =>
        BadRequest
    }
  }
}

