"""SQLAlchemy models for checklists."""

from __future__ import annotations

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import relationship

from db.base import Base


class ChecklistTemplate(Base):
    __tablename__ = "checklist_templates"

    template_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    status = Column(String, nullable=False, default="active", server_default="active")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_employee_id = Column(Integer, ForeignKey("employee.employee_id", ondelete="SET NULL"), nullable=True)

    items = relationship(
        "ChecklistTemplateItem",
        back_populates="template",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="ChecklistTemplateItem.display_order",
    )


class ChecklistTemplateItem(Base):
    __tablename__ = "checklist_template_items"

    item_id = Column(Integer, primary_key=True)
    template_id = Column(Integer, ForeignKey("checklist_templates.template_id", ondelete="CASCADE"), nullable=False)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    display_order = Column(Integer, nullable=True)

    template = relationship("ChecklistTemplate", back_populates="items")


class EngagementChecklist(Base):
    __tablename__ = "engagement_checklists"

    checklist_id = Column(Integer, primary_key=True)
    engagement_id = Column(Integer, ForeignKey("engagements.engagement_id", ondelete="CASCADE"), nullable=False)
    template_id = Column(Integer, ForeignKey("checklist_templates.template_id"), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    created_employee_id = Column(Integer, ForeignKey("employee.employee_id", ondelete="SET NULL"), nullable=True)

    template = relationship("ChecklistTemplate")
    tasks = relationship(
        "EngagementChecklistTask",
        back_populates="checklist",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class EngagementChecklistTask(Base):
    __tablename__ = "engagement_checklist_tasks"

    task_id = Column(Integer, primary_key=True)
    checklist_id = Column(
        Integer,
        ForeignKey("engagement_checklists.checklist_id", ondelete="CASCADE"),
        nullable=False,
    )
    item_id = Column(Integer, ForeignKey("checklist_template_items.item_id"), nullable=False)
    assigned_employee_id = Column(Integer, ForeignKey("employee.employee_id", ondelete="SET NULL"), nullable=True)
    status = Column(String, nullable=False, default="pending", server_default="pending")
    notes = Column(Text, nullable=True)
    due_date = Column(Date, nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    completed_by_employee_id = Column(Integer, ForeignKey("employee.employee_id", ondelete="SET NULL"), nullable=True)

    checklist = relationship("EngagementChecklist", back_populates="tasks")
    item = relationship("ChecklistTemplateItem", foreign_keys=[item_id])
